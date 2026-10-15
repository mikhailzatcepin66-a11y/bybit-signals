import asyncio
import json
import logging
import os
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import pandas as pd
import pandas_ta_classic as ta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pybit.unified_trading import HTTP, WebSocket as BybitWebSocket

import news
import strategy


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("bybit-signals")

SYMBOLS = (
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT",
)
INTERVALS = (strategy.TIMEFRAME,)
HISTORY_LIMIT = 300
STATIC_DIR = Path(__file__).parent / "static"
STATE_PATH = Path(__file__).parent / "signals_state.json"
DAY_MS = 86_400_000


def fetch_daily_closes(symbol: str, before_ms: int) -> list[float] | None:
    """Закрытия полных дневных свечей (UTC) строго до before_ms, по возрастанию времени."""
    try:
        response = HTTP(testnet=False).get_kline(category="linear", symbol=symbol, interval="D", limit=120)
        rows = [r for r in response["result"]["list"] if int(r[0]) < before_ms]
        return [float(r[4]) for r in reversed(rows)]
    except Exception:
        logger.exception("Could not load daily candles for %s", symbol)
        return None


@dataclass
class Candle:
    start: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class SignalEngine:
    def __init__(self) -> None:
        self.candles: dict[tuple[str, int], list[Candle]] = defaultdict(list)
        self.prices: dict[str, float] = {}
        self.signals: list[dict[str, Any]] = []
        self.clients: set[WebSocket] = set()
        self.streams: list[BybitWebSocket] = []
        self.loop: asyncio.AbstractEventLoop | None = None
        self.lock = threading.RLock()
        self.event_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None

    def load_history(self, symbol: str, interval: int, rows: list[list[str]]) -> None:
        # Bybit REST returns newest first; maintain ascending candle time internally.
        history = [
            Candle(
                start=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
            )
            for row in reversed(rows)
        ]
        with self.lock:
            self.candles[(symbol, interval)] = history[-HISTORY_LIMIT:]

    def receive_kline(self, message: dict[str, Any]) -> None:
        if self.loop is None:
            return
        self.loop.call_soon_threadsafe(self._process_kline, message)

    def receive_ticker(self, message: dict[str, Any]) -> None:
        if self.loop is None:
            return
        self.loop.call_soon_threadsafe(self._process_ticker, message)

    def _process_kline(self, message: dict[str, Any]) -> None:
        try:
            topic_parts = message["topic"].split(".")
            interval = int(topic_parts[1])
            symbol = topic_parts[2]
            for item in message["data"]:
                candle = Candle(
                    start=int(item["start"]),
                    open=float(item["open"]),
                    high=float(item["high"]),
                    low=float(item["low"]),
                    close=float(item["close"]),
                    volume=float(item["volume"]),
                )
                key = (symbol, interval)
                history = self.candles[key]
                if history and history[-1].start == candle.start:
                    history[-1] = candle
                elif not history or candle.start > history[-1].start:
                    history.append(candle)
                    del history[:-HISTORY_LIMIT]
                else:
                    continue

                if item.get("confirm"):
                    self._create_signal_if_valid(symbol, interval, history)
        except (KeyError, TypeError, ValueError, IndexError):
            logger.exception("Could not process Bybit kline message: %r", message)

    def _create_signal_if_valid(
        self, symbol: str, interval: int, candles: list[Candle]
    ) -> None:
        frame = pd.DataFrame(
            {
                "open": [c.open for c in candles],
                "high": [c.high for c in candles],
                "low": [c.low for c in candles],
                "close": [c.close for c in candles],
                "volume": [c.volume for c in candles],
            }
        )
        setup = strategy.pullback_signal(frame)
        if setup is None:
            return
        current = candles[-1]
        key = f"{symbol}:{interval}:{current.start}"
        if any(signal["candle_key"] == key for signal in self.signals):
            return
        if any(
            s["symbol"] == symbol and s["status"] == "ACTIVE" for s in self.signals
        ):
            return

        daily = fetch_daily_closes(symbol, current.start // DAY_MS * DAY_MS)
        if daily is not None and not strategy.daily_trend_ok(setup["side"], daily):
            logger.info("Skipped %s %s signal: against the daily trend", setup["side"], symbol)
            return

        closes_at = current.start + interval * 60_000
        signal = {
            "id": key,
            "candle_key": key,
            "symbol": symbol,
            "interval": interval,
            **setup,
            "take_profits": [setup["take_profit"]],
            "original_stop_loss": setup["stop_loss"],
            "max_entry": setup["entry"] + (1 if setup["side"] == "LONG" else -1) * 0.25 * setup["risk"],
            "expires_at": closes_at + strategy.MAX_BARS * interval * 60_000,
            "status": "ACTIVE",
            "breakeven": False,
            "hit_targets": [],
            "result_r": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "news": news.upcoming_event(),
        }
        self.signals.insert(0, signal)
        del self.signals[100:]
        self._broadcast({"type": "signal", "signal": signal})
        logger.info("Generated %s signal for %s (%sm)", setup["side"], symbol, interval)

    def _process_ticker(self, message: dict[str, Any]) -> None:
        try:
            data = message["data"]
            symbol = data["symbol"]
            price = float(data["lastPrice"])
            self.prices[symbol] = price
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            for signal in self.signals:
                if signal["symbol"] != symbol or signal["status"] != "ACTIVE":
                    continue
                is_long = signal["side"] == "LONG"
                risk = signal["risk"]
                sign = 1 if is_long else -1
                if (price <= signal["stop_loss"]) if is_long else (price >= signal["stop_loss"]):
                    signal["status"] = "STOPPED"
                    signal["result_r"] = -1.0
                elif (price >= signal["take_profit"]) if is_long else (price <= signal["take_profit"]):
                    signal["status"] = "COMPLETED"
                    signal["hit_targets"] = [1]
                    signal["result_r"] = strategy.TP_R
                elif now_ms >= signal["expires_at"]:
                    signal["status"] = "EXPIRED"
                    signal["result_r"] = sign * (price - signal["entry"]) / risk
                else:
                    continue
                signal["exit_price"] = price
                self._broadcast({"type": "signal_update", "signal": signal})
            self._broadcast({"type": "ticker", "symbol": symbol, "price": price})
        except (KeyError, TypeError, ValueError):
            logger.exception("Could not process Bybit ticker message: %r", message)

    def save_state(self) -> None:
        try:
            tmp = STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.signals, ensure_ascii=False), encoding="utf-8")
            tmp.replace(STATE_PATH)
        except OSError:
            logger.exception("Could not save signals state")

    def load_state(self) -> int:
        """Восстанавливает сигналы после перезапуска; возвращает число открытых."""
        try:
            saved = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return 0
        except (OSError, ValueError):
            logger.exception("Could not read signals state")
            return 0
        self.signals = [s for s in saved if isinstance(s, dict) and "id" in s][:100]
        return sum(1 for s in self.signals if s.get("status") == "ACTIVE")

    def _broadcast(self, message: dict[str, Any]) -> None:
        if message.get("type") in ("signal", "signal_update"):
            self.save_state()
        if self.loop is not None:
            if self.event_handler is not None:
                asyncio.create_task(self.event_handler(message))
            if self.clients:
                asyncio.create_task(self._send_to_clients(message))

    async def _send_to_clients(self, message: dict[str, Any]) -> None:
        disconnected: list[WebSocket] = []
        for client in list(self.clients):
            try:
                await client.send_json(message)
            except (RuntimeError, WebSocketDisconnect):
                disconnected.append(client)
        for client in disconnected:
            self.clients.discard(client)

    def snapshot(self) -> dict[str, Any]:
        return {
            "type": "snapshot",
            "prices": self.prices.copy(),
            "signals": [signal.copy() for signal in self.signals],
            "stream_enabled": os.getenv("BYBIT_WS_ENABLED", "true").lower() == "true",
        }


engine = SignalEngine()


def _start_bybit_streams() -> list[BybitWebSocket]:
    ws = BybitWebSocket(testnet=False, channel_type="linear")
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            ws.kline_stream(
                interval=interval,
                symbol=symbol,
                callback=engine.receive_kline,
            )
        ws.ticker_stream(symbol=symbol, callback=engine.receive_ticker)
    return [ws]


async def start_market_data() -> None:
    engine.loop = asyncio.get_running_loop()
    restored = engine.load_state()
    if restored:
        logger.info("Restored %s active signals after restart", restored)
    http = HTTP(testnet=False)
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            try:
                response = await asyncio.to_thread(
                    http.get_kline,
                    category="linear",
                    symbol=symbol,
                    interval=str(interval),
                    limit=HISTORY_LIMIT,
                )
                if response.get("retCode") != 0:
                    raise RuntimeError(f"Bybit API returned {response!r}")
                engine.load_history(symbol, interval, response["result"]["list"])
                logger.info(
                    "Loaded %s %sm candles for %s",
                    len(response["result"]["list"]),
                    interval,
                    symbol,
                )
            except Exception:
                logger.exception(
                    "Failed to preload public candle history for %s %sm",
                    symbol,
                    interval,
                )
    try:
        engine.streams = await asyncio.to_thread(_start_bybit_streams)
    except Exception:
        logger.exception("Failed to start Bybit public WebSocket streams")
        raise


def stop_market_data() -> None:
    for stream in engine.streams:
        stream.exit()
    engine.streams.clear()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if os.getenv("BYBIT_WS_ENABLED", "true").lower() == "true":
        await start_market_data()
    yield
    stop_market_data()


app = FastAPI(title="Bybit Live Signals", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    engine.clients.add(websocket)
    await websocket.send_json(engine.snapshot())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        engine.clients.discard(websocket)
