import asyncio
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


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("bybit-signals")

SYMBOLS = ("BTCUSDT", "ETHUSDT")
INTERVALS = (5, 15)
HISTORY_LIMIT = 300
STATIC_DIR = Path(__file__).parent / "static"


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
        if len(candles) < 210:
            return
        frame = pd.DataFrame(
            {
                "open": [c.open for c in candles],
                "high": [c.high for c in candles],
                "low": [c.low for c in candles],
                "close": [c.close for c in candles],
                "volume": [c.volume for c in candles],
            }
        )
        close = frame["close"]
        ema50 = ta.ema(close, length=50)
        ema200 = ta.ema(close, length=200)
        rsi = ta.rsi(close, length=14)
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        atr = ta.atr(frame["high"], frame["low"], close, length=14)
        if macd is None:
            return

        macd_line = macd[next(name for name in macd.columns if name.startswith("MACD_"))]
        macd_signal = macd[next(name for name in macd.columns if name.startswith("MACDs_"))]
        current_index = len(frame) - 1
        previous_index = current_index - 1
        required = (
            ema50.iloc[current_index],
            ema200.iloc[current_index],
            rsi.iloc[current_index],
            rsi.iloc[previous_index],
            macd_line.iloc[current_index],
            macd_line.iloc[previous_index],
            macd_signal.iloc[current_index],
            macd_signal.iloc[previous_index],
            atr.iloc[current_index],
        )
        if any(pd.isna(value) for value in required):
            return

        current = candles[-1]
        prior = candles[-2]
        cross_up = (
            macd_line.iloc[previous_index] <= macd_signal.iloc[previous_index]
            and macd_line.iloc[current_index] > macd_signal.iloc[current_index]
        )
        cross_down = (
            macd_line.iloc[previous_index] >= macd_signal.iloc[previous_index]
            and macd_line.iloc[current_index] < macd_signal.iloc[current_index]
        )
        rising_volume = current.volume > prior.volume
        rsi_now = float(rsi.iloc[current_index])
        rsi_before = float(rsi.iloc[previous_index])
        long_momentum = (rsi_before < 35 <= rsi_now) or (
            45 <= rsi_now <= 65 and rising_volume
        )
        short_momentum = (rsi_before > 65 >= rsi_now) or (
            35 <= rsi_now <= 55
        )

        side: str | None = None
        if current.close > float(ema200.iloc[current_index]) and cross_up and long_momentum:
            side = "LONG"
        elif current.close < float(ema200.iloc[current_index]) and cross_down and short_momentum:
            side = "SHORT"
        if side is None:
            return

        candle_key = f"{symbol}:{interval}:{current.start}"
        if any(signal["candle_key"] == candle_key for signal in self.signals):
            return

        entry = current.close
        volatility_stop = 1.5 * float(atr.iloc[current_index])
        recent = candles[-11:-1]
        if side == "LONG":
            local_extreme = min(c.low for c in recent) if recent else entry - volatility_stop
            stop = min(entry - volatility_stop, local_extreme)
            risk = entry - stop
            targets = [entry + risk * multiple for multiple in (1.5, 2.5, 4.0)]
        else:
            local_extreme = max(c.high for c in recent) if recent else entry + volatility_stop
            stop = max(entry + volatility_stop, local_extreme)
            risk = stop - entry
            targets = [entry - risk * multiple for multiple in (1.5, 2.5, 4.0)]
        if risk <= 0 or not all(pd.notna(value) for value in targets):
            return

        signal = {
            "id": candle_key,
            "candle_key": candle_key,
            "symbol": symbol,
            "interval": interval,
            "side": side,
            "entry": entry,
            "stop_loss": stop,
            "original_stop_loss": stop,
            "take_profits": targets,
            "rsi": rsi_now,
            "ema50": float(ema50.iloc[current_index]),
            "ema200": float(ema200.iloc[current_index]),
            "atr": float(atr.iloc[current_index]),
            "status": "ACTIVE",
            "breakeven": False,
            "hit_targets": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.signals.insert(0, signal)
        del self.signals[100:]
        self._broadcast({"type": "signal", "signal": signal})
        logger.info("Generated %s signal for %s (%sm)", side, symbol, interval)

    def _process_ticker(self, message: dict[str, Any]) -> None:
        try:
            data = message["data"]
            symbol = data["symbol"]
            price = float(data["lastPrice"])
            self.prices[symbol] = price
            changed: list[dict[str, Any]] = []
            for signal in self.signals:
                if signal["symbol"] != symbol or signal["status"] != "ACTIVE":
                    continue
                is_long = signal["side"] == "LONG"
                targets = signal["take_profits"]
                hit_targets = signal["hit_targets"]
                previous_hit_count = len(hit_targets)
                previous_status = signal["status"]
                previous_stop = signal["stop_loss"]
                for index, target in enumerate(targets):
                    reached = price >= target if is_long else price <= target
                    if reached and index + 1 not in hit_targets:
                        hit_targets.append(index + 1)
                        if index == 0 and not signal["breakeven"]:
                            signal["breakeven"] = True
                            signal["stop_loss"] = signal["entry"]
                stopped = price <= signal["stop_loss"] if is_long else price >= signal["stop_loss"]
                if stopped:
                    signal["status"] = "STOPPED" if not signal["breakeven"] else "BREAKEVEN"
                elif len(hit_targets) == 3:
                    signal["status"] = "COMPLETED"
                if (
                    len(hit_targets) != previous_hit_count
                    or signal["status"] != previous_status
                    or signal["stop_loss"] != previous_stop
                ):
                    changed.append(signal)
            for signal in changed:
                self._broadcast({"type": "signal_update", "signal": signal})
            self._broadcast({"type": "ticker", "symbol": symbol, "price": price})
        except (KeyError, TypeError, ValueError):
            logger.exception("Could not process Bybit ticker message: %r", message)

    def _broadcast(self, message: dict[str, Any]) -> None:
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
