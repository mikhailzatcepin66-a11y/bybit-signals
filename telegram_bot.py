import asyncio
import logging
import math
import os
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

import strategy
from main import SYMBOLS, engine, start_market_data, stop_market_data


BASE_DIR = Path(__file__).parent
DATABASE_PATH = BASE_DIR / "subscribers.sqlite3"
DEFAULT_DEPOSIT = 20.0
DEFAULT_RISK_PCT = 1.0
FEE_RATE = 0.00055
WIN_RATE = 60
logger = logging.getLogger("bybit-telegram")
router = Router()
dispatcher = Dispatcher()
dispatcher.include_router(router)


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS subscribers ("
                "chat_id INTEGER PRIMARY KEY, subscribed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS settings ("
                "chat_id INTEGER PRIMARY KEY, deposit REAL NOT NULL, risk_pct REAL NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS journal ("
                "signal_id TEXT PRIMARY KEY, symbol TEXT, side TEXT, status TEXT, "
                "result_r REAL, risk_pct REAL, closed_at TEXT DEFAULT CURRENT_TIMESTAMP)"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def subscribe(self, chat_id: int) -> None:
        with self._connect() as db:
            db.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
            db.execute(
                "INSERT OR IGNORE INTO settings (chat_id, deposit, risk_pct) VALUES (?, ?, ?)",
                (chat_id, DEFAULT_DEPOSIT, DEFAULT_RISK_PCT),
            )

    def unsubscribe(self, chat_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))

    def list_chat_ids(self) -> list[int]:
        with self._connect() as db:
            rows = db.execute("SELECT chat_id FROM subscribers ORDER BY subscribed_at").fetchall()
        return [int(r[0]) for r in rows]

    def is_subscribed(self, chat_id: int) -> bool:
        with self._connect() as db:
            return db.execute(
                "SELECT 1 FROM subscribers WHERE chat_id = ?", (chat_id,)
            ).fetchone() is not None

    def get_settings(self, chat_id: int) -> tuple[float, float]:
        with self._connect() as db:
            row = db.execute(
                "SELECT deposit, risk_pct FROM settings WHERE chat_id = ?", (chat_id,)
            ).fetchone()
        return (float(row[0]), float(row[1])) if row else (DEFAULT_DEPOSIT, DEFAULT_RISK_PCT)

    def set_setting(self, chat_id: int, column: str, value: float) -> None:
        if column not in ("deposit", "risk_pct"):
            raise ValueError(column)
        self.subscribe(chat_id)
        with self._connect() as db:
            db.execute(f"UPDATE settings SET {column} = ? WHERE chat_id = ?", (value, chat_id))

    def record_result(self, signal: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO journal (signal_id, symbol, side, status, result_r, risk_pct) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (signal["id"], signal["symbol"], signal["side"], signal["status"],
                 signal["result_r"], signal["risk_pct"]),
            )

    def journal(self) -> list[tuple[str, float, float]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT status, result_r, risk_pct FROM journal ORDER BY closed_at"
            ).fetchall()
        return [(r[0], float(r[1]), float(r[2])) for r in rows]


store = Store(DATABASE_PATH)
_instruments: dict[str, dict[str, float]] = {}
_http = HTTP(testnet=False)


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.1f}"
    if value >= 10:
        return f"{value:,.2f}"
    return f"{value:,.4f}"


def fmt_qty(value: float, step: float) -> str:
    decimals = max(0, -Decimal(str(step)).as_tuple().exponent)
    return f"{value:.{decimals}f}"


async def instrument(symbol: str) -> dict[str, float] | None:
    if symbol in _instruments:
        return _instruments[symbol]
    try:
        response = await asyncio.to_thread(
            _http.get_instruments_info, category="linear", symbol=symbol
        )
        lot = response["result"]["list"][0]["lotSizeFilter"]
        info = {
            "min_qty": float(lot["minOrderQty"]),
            "step": float(lot["qtyStep"]),
            "min_notional": float(lot.get("minNotionalValue") or 5),
        }
    except Exception:
        logger.exception("Could not load instrument info for %s", symbol)
        return None
    _instruments[symbol] = info
    return info


def size_position(signal: dict[str, Any], deposit: float, risk_pct: float, info: dict[str, float]) -> dict[str, Any]:
    entry, distance = signal["entry"], signal["risk"]
    wanted_risk = deposit * risk_pct / 100
    step = info["step"]
    qty = math.floor(wanted_risk / distance / step) * step
    floor_qty = max(info["min_qty"], math.ceil(info["min_notional"] / entry / step) * step)
    forced = qty < floor_qty
    if forced:
        qty = floor_qty
    notional = qty * entry
    loss = qty * distance + notional * FEE_RATE * 2
    gain = qty * distance * strategy.TP_R - notional * FEE_RATE * 2
    max_leverage = max(1, math.floor(60 / signal["risk_pct"]))
    min_leverage = max(1, math.ceil(notional / (deposit * 0.9)))
    return {
        "qty": qty, "notional": notional, "loss": loss, "gain": gain,
        "loss_pct": loss / deposit * 100, "forced": forced,
        "min_leverage": min_leverage, "max_leverage": max_leverage,
        "skip": loss > 2.5 * wanted_risk and loss / deposit * 100 > 3 or min_leverage > max_leverage,
    }


def format_new_signal(signal: dict[str, Any], deposit: float, risk_pct: float, info: dict[str, float] | None) -> str:
    is_long = signal["side"] == "LONG"
    symbol = signal["symbol"]
    coin = symbol.removesuffix("USDT")
    sl_pct = signal["risk_pct"]
    tp_pct = sl_pct * strategy.TP_R
    action = "LONG ? ????????" if is_long else "SHORT ? ???????"
    lines = [
        f"{'??' if is_long else '??'} <b>????? ?????? ? {coin}/USDT</b>",
        f"<b>{action}</b> ? ?????? 4?",
        "",
        "<b>?? ????</b>",
        f"????: <code>{fmt_price(signal['entry'])}</code> (?? ?????)",
        f"????-????: <code>{fmt_price(signal['stop_loss'])}</code>  (?{sl_pct:.2f}%)",
        f"????-??????: <code>{fmt_price(signal['take_profit'])}</code>  (+{tp_pct:.2f}%)",
        f"? ??????? ?????? ???? ???? {'?? ????' if is_long else '?? ????'} "
        f"<code>{fmt_price(signal['max_entry'])}</code>. ?????? ? ?????? ??????, ??????????.",
        "",
    ]
    if info is None:
        lines += [f"<b>?? ??????</b>\n???????? ?? ????? {risk_pct:g}% ????????.", ""]
    else:
        plan = size_position(signal, deposit, risk_pct, info)
        lines += [
            f"<b>?? ??? ??????</b> (??????? ${deposit:g}, ???? {risk_pct:g}%)",
            f"?????: <code>{fmt_qty(plan['qty'], info['step'])}</code> {coin} (?? ${plan['notional']:.2f})",
            f"?????: {plan['min_leverage']}x?{plan['max_leverage']}x, ????? Isolated",
            f"???? ????: <b>?${plan['loss']:.2f}</b> ({plan['loss_pct']:.1f}% ????????)",
            f"???? ????: <b>+${plan['gain']:.2f}</b>",
        ]
        if plan["skip"]:
            lines.append(
                "?? <b>???? ?????? ????? ??????????:</b> ??????????? ????? ????? "
                "??????? ????? ??? ?????? ????????."
            )
        elif plan["forced"]:
            lines.append(
                f"?? ??????????? ????? {coin} ?????? ??????????, ???? ???? {risk_pct:g}%."
            )
        lines.append("")
    lines += [
        "<b>?? ??? ????????? ? Bybit</b>",
        "1. ?????????? ? USDT Perpetual ? ???????? ????.",
        f"2. Isolated, ????? ?? ????????? ????.",
        f"3. ??? Market, ????? ??? ????, ?????? {'Buy / Long' if is_long else 'Sell / Short'}.",
        "4. ???????? TP/SL ? ??????? ? ??????? ???? ????? ? ????? (Mark Price).",
        "5. ?????? ?????? ?? ???????: ????? ??????? ?????? ????.",
        "",
        "<b>?? ???????</b>",
        "? ???? ?? ?????????? ? ?? ???????.",
        f"? ???? ?? {strategy.MAX_BARS // 6} ???? ?????? ?? ?????????, ??? ?????? ??????? ???????.",
        "? ???????????? ?? ?????? 2 ??????.",
        "",
        f"<i>?? ?????? ?? 2 ????: ? {WIN_RATE}% ?????? ? ????. ?????? ????????? ? "
        "??????? ???? ?? ????? ??????, ? ?? ?? ??????.</i>",
    ]
    return "\n".join(lines)


def format_update(signal: dict[str, Any]) -> str:
    coin = signal["symbol"].removesuffix("USDT")
    status = signal["status"]
    if status == "COMPLETED":
        return (
            f"? <b>{coin}: ????-?????? ?????????</b>\n"
            f"???? <code>{fmt_price(signal['take_profit'])}</code>. ?????? ??????? ? ???? (+{strategy.TP_R:g}R).\n"
            "???? TP/SL ?????????? ?? ?????, ??????? ??? ???????. ????? ????????? ??????."
        )
    if status == "STOPPED":
        return (
            f"? <b>{coin}: ???????? ????-????</b>\n"
            f"???? <code>{fmt_price(signal['stop_loss'])}</code>. ?????? ? ???????? ????? (?1R).\n"
            "??? ?????????? ????? ?????????. ?? ???????????? ?????, ????? ??????????."
        )
    return (
        f"? <b>{coin}: ????? ??????? ?????</b>\n"
        f"???????? ??????? ??????? ?? ????? ? ???????? TP/SL. ????????? ? {signal['result_r']:+.2f}R."
    )


async def notify_subscribers(bot: Bot, build_text: Any) -> None:
    for chat_id in store.list_chat_ids():
        try:
            await bot.send_message(chat_id, await build_text(chat_id))
        except TelegramForbiddenError:
            logger.info("Removing blocked subscriber chat %s", chat_id)
            store.unsubscribe(chat_id)
        except TelegramAPIError:
            logger.exception("Could not send notification to chat %s", chat_id)


async def handle_market_event(bot: Bot, event: dict[str, Any]) -> None:
    signal = event.get("signal")
    if event["type"] == "signal":
        info = await instrument(signal["symbol"])

        async def build(chat_id: int) -> str:
            deposit, risk_pct = store.get_settings(chat_id)
            return format_new_signal(signal, deposit, risk_pct, info)

        await notify_subscribers(bot, build)
    elif event["type"] == "signal_update":
        store.record_result(signal)
        text = format_update(signal)

        async def build(_: int) -> str:
            return text

        await notify_subscribers(bot, build)


HELP = (
    "<b>Бот сигналов Bybit</b> (стратегия «откат по тренду», 4h)\n"
    f"Монеты: {', '.join(s.removesuffix('USDT') for s in SYMBOLS)}\n\n"
    "/status — цены и открытые сигналы\n"
    "/deposit 20 — ваш депозит в $\n"
    "/risk 1 — риск на сделку в % депозита (0.25–2)\n"
    "/stats — результаты закрытых сигналов\n"
    "/stop — отключить уведомления\n\n"
    "Сигналов немного (в среднем 1–3 в неделю), каждый приходит с готовой инструкцией."
)


@router.message(CommandStart())
async def start_command(message: Message) -> None:
    if message.chat.type != "private":
        await message.answer("Откройте бота в личном чате и отправьте /start.")
        return
    store.subscribe(message.chat.id)
    deposit, risk_pct = store.get_settings(message.chat.id)
    await message.answer(
        f"Вы подписаны. Депозит ${deposit:g}, риск {risk_pct:g}% на сделку.\n"
        "Измените: /deposit 100 и /risk 1.\n\n" + HELP
    )


@router.message(Command("stop"))
async def stop_command(message: Message) -> None:
    store.unsubscribe(message.chat.id)
    await message.answer("Уведомления отключены. Подписка снова: /start.")


@router.message(Command("help"))
async def help_command(message: Message) -> None:
    await message.answer(HELP)


def _parse_number(command: CommandObject) -> float | None:
    try:
        value = float((command.args or "").replace(",", ".").strip())
    except ValueError:
        return None
    return value if math.isfinite(value) else None


@router.message(Command("deposit"))
async def deposit_command(message: Message, command: CommandObject) -> None:
    value = _parse_number(command)
    if value is None or not 5 <= value <= 1_000_000:
        await message.answer("Пример: /deposit 20 (сумма в $, от 5).")
        return
    store.set_setting(message.chat.id, "deposit", value)
    await message.answer(f"Депозит: ${value:g}.")


@router.message(Command("risk"))
async def risk_command(message: Message, command: CommandObject) -> None:
    value = _parse_number(command)
    if value is None or not 0.25 <= value <= 2:
        await message.answer("Пример: /risk 1 (процент депозита на сделку, от 0.25 до 2).")
        return
    store.set_setting(message.chat.id, "risk_pct", value)
    await message.answer(f"Риск на сделку: {value:g}% депозита.")


@router.message(Command("stats"))
async def stats_command(message: Message) -> None:
    rows = store.journal()
    if not rows:
        await message.answer("Закрытых сигналов пока нет.")
        return
    results = [r for _, r, _ in rows]
    wins = sum(1 for r in results if r > 0)
    total = sum(results)
    equity = peak = drawdown = 0.0
    for r in results:
        equity += r
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    await message.answer(
        f"<b>Закрытых сигналов:</b> {len(results)}\n"
        f"В плюс: {wins} ({wins / len(results) * 100:.0f}%)\n"
        f"Сумма: {total:+.1f}R · среднее {total / len(results):+.2f}R\n"
        f"Макс. просадка: {drawdown:.1f}R\n"
        "<i>1R = сумма риска на сделку, без учёта комиссий.</i>"
    )


@router.message(Command("status"))
async def status_command(message: Message) -> None:
    lines = ["<b>Цены</b>"]
    for symbol in SYMBOLS:
        price = engine.prices.get(symbol)
        lines.append(f"{symbol}: <code>{fmt_price(price) if price else 'ожидание данных'}</code>")
    active = [s for s in engine.signals if s["status"] == "ACTIVE"]
    if active:
        lines.append("\n<b>Открытые сигналы:</b>")
        for s in active:
            lines.append(
                f"{s['side']} {s['symbol']} · вход <code>{fmt_price(s['entry'])}</code> · "
                f"SL <code>{fmt_price(s['stop_loss'])}</code> · TP <code>{fmt_price(s['take_profit'])}</code>"
            )
    else:
        lines.append("\nОткрытых сигналов нет — ждём подходящую свечу 4h.")
    deposit, risk_pct = store.get_settings(message.chat.id)
    lines.append(f"\nДепозит ${deposit:g} · риск {risk_pct:g}%")
    await message.answer("\n".join(lines))


async def run() -> None:
    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не задан BOT_TOKEN. Укажите его в файле .env как BOT_TOKEN=ваш_токен.")

    async with Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) as bot:
        engine.event_handler = lambda event: handle_market_event(bot, event)
        try:
            await start_market_data()
            logger.info("Telegram bot started. Send /start to subscribe.")
            await dispatcher.start_polling(bot)
        finally:
            engine.event_handler = None
            stop_market_data()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Telegram bot stopped")
