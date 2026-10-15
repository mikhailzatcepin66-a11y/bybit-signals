import asyncio
import logging
import math
import os
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram import F
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
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
PREF_DEFAULTS = {"tz": 3, "quiet_start": -1, "quiet_end": -1, "loss_limit": 2.0, "week_limit": 4.0}
BACKUP_DIR = BASE_DIR / "backups"
BACKUP_KEEP = 7
REPORT_HOUR = 21
REMIND_AGE_HOURS = 72
WATCHDOG_SECONDS = 300
LAST = {"tick": time.monotonic(), "stalled": False}
logger = logging.getLogger("bybit-telegram")
router = Router()
dispatcher = Dispatcher()
dispatcher.include_router(router)


def _utc(text: str) -> datetime:
    return datetime.strptime(str(text)[:19], "%Y-%m-%d %H:%M:%S")


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
                "CREATE TABLE IF NOT EXISTS muted ("
                "chat_id INTEGER NOT NULL, symbol TEXT NOT NULL, PRIMARY KEY (chat_id, symbol))"
            )
            for column, definition in (
                ("tz", "INTEGER NOT NULL DEFAULT 3"),
                ("quiet_start", "INTEGER NOT NULL DEFAULT -1"),
                ("quiet_end", "INTEGER NOT NULL DEFAULT -1"),
                ("loss_limit", "REAL NOT NULL DEFAULT 2.0"),
                ("week_limit", "REAL NOT NULL DEFAULT 4.0"),
            ):
                try:
                    db.execute(f"ALTER TABLE settings ADD COLUMN {column} {definition}")
                except sqlite3.OperationalError:
                    pass
            db.execute(
                "CREATE TABLE IF NOT EXISTS trades ("
                "chat_id INTEGER NOT NULL, signal_id TEXT NOT NULL, taken INTEGER NOT NULL, "
                "PRIMARY KEY (chat_id, signal_id))"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS reminders ("
                "chat_id INTEGER NOT NULL, key TEXT NOT NULL, PRIMARY KEY (chat_id, key))"
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


    def get_prefs(self, chat_id: int) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute(
                "SELECT tz, quiet_start, quiet_end, loss_limit, week_limit FROM settings WHERE chat_id = ?", (chat_id,)
            ).fetchone()
        if not row:
            return dict(PREF_DEFAULTS)
        return {"tz": int(row[0]), "quiet_start": int(row[1]), "quiet_end": int(row[2]),
                "loss_limit": float(row[3]), "week_limit": float(row[4])}

    def set_pref(self, chat_id: int, **values: float) -> None:
        if not set(values) <= set(PREF_DEFAULTS):
            raise ValueError(values)
        self.subscribe(chat_id)
        with self._connect() as db:
            for column, value in values.items():
                db.execute(f"UPDATE settings SET {column} = ? WHERE chat_id = ?", (value, chat_id))

    def mark_trade(self, chat_id: int, signal_id: str, taken: int) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO trades (chat_id, signal_id, taken) VALUES (?, ?, ?)",
                (chat_id, signal_id, taken),
            )

    def trade_state(self, chat_id: int, signal_id: str) -> int | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT taken FROM trades WHERE chat_id = ? AND signal_id = ?", (chat_id, signal_id)
            ).fetchone()
        return int(row[0]) if row else None

    def taken_by(self, signal_id: str) -> list[int]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT chat_id FROM trades WHERE signal_id = ? AND taken = 1", (signal_id,)
            ).fetchall()
        return [int(r[0]) for r in rows]

    def closed_trades(self, chat_id: int) -> list[tuple[datetime, float]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT j.closed_at, j.result_r FROM trades t JOIN journal j ON j.signal_id = t.signal_id "
                "WHERE t.chat_id = ? AND t.taken = 1 ORDER BY j.closed_at", (chat_id,)
            ).fetchall()
        return [(_utc(r[0]), float(r[1])) for r in rows]

    def all_closed(self) -> list[tuple[datetime, float]]:
        with self._connect() as db:
            rows = db.execute("SELECT closed_at, result_r FROM journal ORDER BY closed_at").fetchall()
        return [(_utc(r[0]), float(r[1])) for r in rows]

    def open_taken(self, chat_id: int) -> list[str]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT signal_id FROM trades WHERE chat_id = ? AND taken = 1 "
                "AND signal_id NOT IN (SELECT signal_id FROM journal)", (chat_id,)
            ).fetchall()
        return [r[0] for r in rows]

    def reminded(self, chat_id: int, key: str) -> bool:
        with self._connect() as db:
            return db.execute(
                "SELECT 1 FROM reminders WHERE chat_id = ? AND key = ?", (chat_id, key)
            ).fetchone() is not None

    def mark_reminded(self, chat_id: int, key: str) -> None:
        with self._connect() as db:
            db.execute("INSERT OR IGNORE INTO reminders (chat_id, key) VALUES (?, ?)", (chat_id, key))

    def muted(self, chat_id: int) -> set[str]:
        with self._connect() as db:
            rows = db.execute("SELECT symbol FROM muted WHERE chat_id = ?", (chat_id,)).fetchall()
        return {r[0] for r in rows}

    def toggle_coin(self, chat_id: int, symbol: str) -> None:
        with self._connect() as db:
            cursor = db.execute("DELETE FROM muted WHERE chat_id = ? AND symbol = ?", (chat_id, symbol))
            if cursor.rowcount == 0:
                db.execute("INSERT INTO muted (chat_id, symbol) VALUES (?, ?)", (chat_id, symbol))

    def set_all_coins(self, chat_id: int, enabled: bool) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM muted WHERE chat_id = ?", (chat_id,))
            if not enabled:
                db.executemany(
                    "INSERT INTO muted (chat_id, symbol) VALUES (?, ?)", [(chat_id, s) for s in SYMBOLS]
                )

    def recent(self, limit: int = 10) -> list[tuple[str, str, str, float, str]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT symbol, side, status, result_r, closed_at FROM journal "
                "ORDER BY closed_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [(r[0], r[1], r[2], float(r[3]), str(r[4])) for r in rows]


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
    action = "LONG — покупка" if is_long else "SHORT — продажа"
    lines = [
        f"{'🟢' if is_long else '🔴'} <b>Новый сигнал — {coin}/USDT</b>",
        f"<b>{action}</b> · свечи 4ч",
        "",
        "<b>📍 Цены</b>",
        f"Вход: <code>{fmt_price(signal['entry'])}</code> (по рынку)",
        f"Стоп-лосс: <code>{fmt_price(signal['stop_loss'])}</code>  (−{sl_pct:.2f}%)",
        f"Тейк-профит: <code>{fmt_price(signal['take_profit'])}</code>  (+{tp_pct:.2f}%)",
        f"⏱ Входить можно, пока цена {'не выше' if is_long else 'не ниже'} "
        f"<code>{fmt_price(signal['max_entry'])}</code>. Дальше — вход упущен, пропустите.",
        "",
    ]
    if info is None:
        lines += [f"<b>💵 Размер</b>\nРискуйте не более {risk_pct:g}% депозита.", ""]
    else:
        plan = size_position(signal, deposit, risk_pct, info)
        lines += [
            f"<b>💵 Ваш размер</b> (депозит ${deposit:g}, риск {risk_pct:g}%)",
            f"Объём: <code>{fmt_qty(plan['qty'], info['step'])}</code> {coin} (≈ ${plan['notional']:.2f})",
            f"Плечо: {plan['min_leverage']}x–{plan['max_leverage']}x, режим Isolated",
            f"Если стоп: <b>−${plan['loss']:.2f}</b> ({plan['loss_pct']:.1f}% депозита)",
            f"Если тейк: <b>+${plan['gain']:.2f}</b>",
        ]
        if plan["skip"]:
            lines.append(
                "⚠️ <b>Этот сигнал лучше пропустить:</b> минимальный объём биржи "
                "слишком велик для вашего депозита."
            )
        elif plan["forced"]:
            lines.append(
                f"⚠️ Минимальный объём {coin} больше расчётного, риск выше {risk_pct:g}%."
            )
        lines.append("")
    lines += [
        "<b>🛠 Как поставить в Bybit</b>",
        "1. Деривативы → USDT Perpetual → выберите пару.",
        "2. Isolated, плечо из диапазона выше.",
        f"3. Тип Market, объём как выше, кнопка {'Buy / Long' if is_long else 'Sell / Short'}.",
        "4. Включите TP/SL, укажите цены стопа и тейка (Mark Price).",
        "5. Больше ничего не нужно: биржа сама закроет сделку.",
        "",
        "<b>📋 Правила</b>",
        "• Стоп не двигайте и не убирайте.",
        f"• Если за {strategy.MAX_BARS // 6} дней сделка не закрылась, закройте вручную.",
        "• Одновременно не больше 2 сделок.",
        "",
        f"<i>📊 В тесте за 2 года: ≈{WIN_RATE}% сделок в плюс. Прибыль достигается "
        "серией сделок, а не каждой отдельной.</i>",
    ]
    return "\n".join(lines)


def format_update(signal: dict[str, Any]) -> str:
    coin = signal["symbol"].removesuffix("USDT")
    status = signal["status"]
    if status == "COMPLETED":
        return (
            f"✅ <b>{coin}: тейк-профит достигнут</b>\n"
            f"Цена <code>{fmt_price(signal['take_profit'])}</code>. Сделка закрыта в плюс (+{strategy.TP_R:g}R).\n"
            "Если TP/SL выставлены на бирже, закрытие произошло само. Ждите следующий сигнал."
        )
    if status == "STOPPED":
        return (
            f"❌ <b>{coin}: сработал стоп-лосс</b>\n"
            f"Цена <code>{fmt_price(signal['stop_loss'])}</code>. Сделка закрыта с ожидаемым убытком (−1R).\n"
            "Это нормальная часть стратегии. Не пытайтесь отыграться, ждите следующий сигнал."
        )
    return (
        f"⏹ <b>{coin}: время сделки истекло</b>\n"
        f"Закройте позицию вручную по рынку и отмените TP/SL. Результат ≈ {signal['result_r']:+.2f}R."
    )


def local_now(tz: int) -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=tz)


def in_quiet(prefs: dict[str, Any]) -> bool:
    start, end = prefs["quiet_start"], prefs["quiet_end"]
    if start < 0 or start == end:
        return False
    hour = local_now(prefs["tz"]).hour
    return start <= hour < end if start < end else hour >= start or hour < end


def period_start(tz: int, days: int) -> datetime:
    today = local_now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=days - 1)


def week_start(tz: int) -> datetime:
    today = local_now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=today.weekday())


def my_results(chat_id: int, days: int | None = None, since: datetime | None = None) -> list[float]:
    prefs = store.get_prefs(chat_id)
    tz = prefs["tz"]
    start = since or (period_start(tz, days) if days else None)
    return [r for dt, r in store.closed_trades(chat_id) if start is None or dt + timedelta(hours=tz) >= start]


def pause_reason(chat_id: int) -> str | None:
    prefs = store.get_prefs(chat_id)
    week = prefs["week_limit"]
    if week > 0 and sum(my_results(chat_id, since=week_start(prefs["tz"]))) <= -week:
        return "week"
    day = prefs["loss_limit"]
    if day > 0 and sum(my_results(chat_id, 1)) <= -day:
        return "day"
    return None


def is_paused(chat_id: int) -> bool:
    return pause_reason(chat_id) is not None


def feed_status() -> str:
    age = time.monotonic() - LAST["tick"]
    if age <= WATCHDOG_SECONDS:
        return "🟢 Бот работает, данные Bybit идут."
    return f"🔴 Нет данных от Bybit уже {age / 60:.0f} мин."


def format_report(signal: dict[str, Any], deposit: float, risk_pct: float, chat_id: int) -> str:
    if store.trade_state(chat_id, signal["id"]) != 1:
        return "ℹ️ Вы не отметили вход по этому сигналу — в ваш личный отчёт он не попал."
    results = my_results(chat_id)
    wins = sum(1 for r in results if r > 0)
    money = signal["result_r"] * deposit * risk_pct / 100
    verdict = "✅ Успешно" if signal["result_r"] > 0 else "❌ Неудачно"
    total_money = sum(results) * deposit * risk_pct / 100
    text = (
        "<b>📑 Ваш отчёт по сигналу</b>\n"
        f"{verdict}: {signal['side']} {signal['symbol'].removesuffix('USDT')} · "
        f"{signal['result_r']:+.2f}R ≈ <b>{money:+.2f}$</b> (депозит ${deposit:g}, риск {risk_pct:g}%)\n"
        f"<b>Всего ваших сделок:</b> {len(results)} · в плюс {wins} ({wins / len(results) * 100:.0f}%) · "
        f"итог {sum(results):+.1f}R ≈ {total_money:+.2f}$"
    )
    reason = pause_reason(chat_id)
    prefs = store.get_prefs(chat_id)
    if reason == "week":
        text += (
            f"\n\n⛔ <b>Недельный лимит убытка ({prefs['week_limit']:g}R) достигнут.</b>\n"
            "Пауза до понедельника: не входите в новые сделки, разберите ошибки и вернитесь с чистой головой."
        )
    elif reason == "day":
        text += (
            f"\n\n⛔ <b>Дневной лимит убытка ({prefs['loss_limit']:g}R) достигнут.</b>\n"
            "На сегодня хватит: не входите в новые сделки и не пытайтесь отыграться. Завтра — с чистого листа."
        )
    return text


def format_period_report(chat_id: int, days: int) -> tuple[str, bool]:
    deposit, risk_pct = store.get_settings(chat_id)
    tz = store.get_prefs(chat_id)["tz"]
    start = period_start(tz, days)
    everyone = [r for dt, r in store.all_closed() if dt + timedelta(hours=tz) >= start]
    mine = my_results(chat_id, days)
    opened = store.open_taken(chat_id)
    lines = ["<b>📅 Итоги дня</b>" if days == 1 else "<b>🗓 Итоги недели</b>"]
    lines.append(
        f"Сигналов закрыто ботом: {len(everyone)} · в плюс {sum(1 for r in everyone if r > 0)} · "
        f"сумма {sum(everyone):+.1f}R"
    )
    if mine:
        lines.append(
            f"Ваши сделки: {len(mine)} · в плюс {sum(1 for r in mine if r > 0)} · "
            f"итог {sum(mine):+.1f}R ≈ <b>{sum(mine) * deposit * risk_pct / 100:+.2f}$</b>"
        )
    else:
        lines.append("Ваши сделки: закрытых нет (отмечайте «Я вошёл» под сигналом).")
    if opened:
        lines.append(f"Открыто сейчас: {len(opened)}")
    lines.append(feed_status())
    return "\n".join(lines), bool(everyone or mine or opened)


def trade_keyboard(signal_id: str, state: int | None) -> InlineKeyboardMarkup:
    yes = "✅ Я вошёл" + (" ✔" if state == 1 else "")
    no = "⏭ Пропустил" + (" ✔" if state == 0 else "")
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=yes, callback_data=f"take:{signal_id}"),
        InlineKeyboardButton(text=no, callback_data=f"skip:{signal_id}"),
    ]])


async def send_message(bot: Bot, chat_id: int, text: str, markup: InlineKeyboardMarkup | None = None) -> bool:
    silent = in_quiet(store.get_prefs(chat_id))
    try:
        await bot.send_message(chat_id, text, reply_markup=markup, disable_notification=silent)
        return True
    except TelegramForbiddenError:
        logger.info("Removing blocked subscriber chat %s", chat_id)
        store.unsubscribe(chat_id)
    except TelegramAPIError:
        logger.exception("Could not send notification to chat %s", chat_id)
    return False


async def notify_subscribers(
    bot: Bot, build_text: Any, symbol: str | None = None, signal_id: str | None = None
) -> None:
    for chat_id in store.list_chat_ids():
        took = signal_id is not None and store.trade_state(chat_id, signal_id) == 1
        if symbol and symbol in store.muted(chat_id) and not took:
            continue
        built = await build_text(chat_id)
        text, markup = built if isinstance(built, tuple) else (built, None)
        await send_message(bot, chat_id, text, markup)


async def handle_market_event(bot: Bot, event: dict[str, Any]) -> None:
    if event["type"] == "ticker":
        LAST["tick"] = time.monotonic()
        return
    signal = event.get("signal")
    if event["type"] == "signal":
        info = await instrument(signal["symbol"])

        async def build(chat_id: int) -> Any:
            deposit, risk_pct = store.get_settings(chat_id)
            text = format_new_signal(signal, deposit, risk_pct, info)
            if signal.get("news"):
                text = (
                    f"📰 <b>Рядом событие: {signal['news']['name']}.</b> Рынок может резко дёрнуться, "
                    "стоп сработает чаще обычного. Осторожнее: лучше пропустить или взять половину объёма.\n\n"
                ) + text
            reason = pause_reason(chat_id)
            if reason:
                period = "Недельный" if reason == "week" else "Дневной"
                return f"⛔ <b>{period} лимит убытка достигнут — этот сигнал пропустите.</b>\n\n" + text
            text += (
                "\n\n<i>Если войдёте, нажмите «Я вошёл»: сделка попадёт в вашу статистику, "
                "и бот напомнит о ней.</i>"
            )
            return text, trade_keyboard(signal["id"], None)

        await notify_subscribers(bot, build, signal["symbol"])
    elif event["type"] == "signal_update":
        store.record_result(signal)
        text = format_update(signal)

        async def build(chat_id: int) -> str:
            deposit, risk_pct = store.get_settings(chat_id)
            return text + "\n\n" + format_report(signal, deposit, risk_pct, chat_id)

        await notify_subscribers(bot, build, signal["symbol"], signal["id"])


async def check_feed(bot: Bot) -> None:
    age = time.monotonic() - LAST["tick"]
    if age > WATCHDOG_SECONDS and not LAST["stalled"]:
        LAST["stalled"] = True
        text = (
            "🔴 <b>Нет данных от Bybit более 5 минут.</b>\n"
            "Новые сигналы и закрытия сделок временно не приходят. Следите за открытыми сделками на бирже."
        )
    elif age <= WATCHDOG_SECONDS and LAST["stalled"]:
        LAST["stalled"] = False
        text = "🟢 <b>Данные Bybit восстановлены.</b> Бот снова работает."
    else:
        return

    async def build(_: int) -> str:
        return text

    await notify_subscribers(bot, build)


async def send_reminders(bot: Bot) -> None:
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    for signal in engine.signals:
        if signal["status"] != "ACTIVE":
            continue
        coin = signal["symbol"].removesuffix("USDT")
        age_h = (now - datetime.fromisoformat(signal["created_at"])).total_seconds() / 3600
        left_h = (signal["expires_at"] - now_ms) / 3_600_000
        reminders = (
            (f"age:{signal['id']}", age_h >= REMIND_AGE_HOURS,
             f"⏰ <b>{coin}: сделка открыта уже {age_h / 24:.0f} дн.</b>\n"
             f"Проверьте, что на бирже стоят SL <code>{fmt_price(signal['stop_loss'])}</code> и "
             f"TP <code>{fmt_price(signal['take_profit'])}</code>. "
             f"Если сделка не закроется сама, через {left_h / 24:.1f} дн. её нужно закрыть вручную."),
            (f"exp:{signal['id']}", left_h <= 24,
             f"⏰ <b>{coin}: меньше суток до закрытия по времени.</b>\n"
             "Если сделка до этого не дойдёт ни до тейка, ни до стопа, закройте её вручную по рынку "
             "и отмените TP/SL."),
        )
        for chat_id in store.taken_by(signal["id"]):
            for key, due, text in reminders:
                if due and not store.reminded(chat_id, key):
                    store.mark_reminded(chat_id, key)
                    await send_message(bot, chat_id, text)


async def send_reports(bot: Bot) -> None:
    for chat_id in store.list_chat_ids():
        now = local_now(store.get_prefs(chat_id)["tz"])
        if now.hour < REPORT_HOUR:
            continue
        day_key = f"daily:{now:%Y-%m-%d}"
        if not store.reminded(chat_id, day_key):
            store.mark_reminded(chat_id, day_key)
            text, active = format_period_report(chat_id, 1)
            if active:
                await send_message(bot, chat_id, text)
        week_key = f"weekly:{now:%G-%V}"
        if now.weekday() == 6 and not store.reminded(chat_id, week_key):
            store.mark_reminded(chat_id, week_key)
            await send_message(bot, chat_id, format_period_report(chat_id, 7)[0])


def backup_data() -> None:
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    target = BACKUP_DIR / f"subscribers-{stamp}.sqlite3"
    if target.exists():
        return
    source = sqlite3.connect(DATABASE_PATH)
    try:
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()
    state = BASE_DIR / "signals_state.json"
    if state.exists():
        shutil.copy2(state, BACKUP_DIR / f"signals-{stamp}.json")
    for pattern in ("subscribers-*.sqlite3", "signals-*.json"):
        for old in sorted(BACKUP_DIR.glob(pattern))[:-BACKUP_KEEP]:
            old.unlink(missing_ok=True)
    logger.info("Backup saved: %s", target.name)


async def background_loop(bot: Bot) -> None:
    while True:
        await asyncio.sleep(60)
        try:
            await asyncio.to_thread(backup_data)
        except Exception:
            logger.exception("Backup failed")
        for step in (check_feed, send_reminders, send_reports):
            try:
                await step(bot)
            except Exception:
                logger.exception("Background step %s failed", step.__name__)


HELP = (
    "<b>Бот сигналов Bybit</b> (стратегия «откат по тренду», 4h)\n"
    f"Монеты: {', '.join(s.removesuffix('USDT') for s in SYMBOLS)}\n\n"
    "/status — цены, открытые сигналы, состояние бота\n"
    "/stats — итоги по закрытым сигналам\n"
    "/history — последние 10 сигналов\n"
    "/report — итоги дня, /week — итоги недели\n"
    "/coins — выбрать монеты\n"
    "/deposit 20 — депозит в $\n"
    "/risk 1 — риск на сделку, % депозита (0.25–2)\n"
    "/limit 2 — дневной лимит убытка в R (0 — выключить)\n"
    "/weeklimit 4 — недельный лимит убытка в R (пауза до понедельника)\n"
    "/quiet 23 7 — тихие часы (без звука), /quiet off — выключить\n"
    "/tz 3 — ваш часовой пояс (UTC+3 — Москва)\n"
    "/stop — отключить уведомления\n\n"
    "Сигналов в среднем 2–4 в неделю. Под каждым сигналом кнопки «Я вошёл» / «Пропустил» — "
    "по ним бот ведёт вашу личную статистику, напоминает об открытых сделках и присылает отчёты "
    "каждый день в 21:00 и в воскресенье итоги недели."
)


MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статус"), KeyboardButton(text="📈 Статистика")],
        [KeyboardButton(text="💰 Депозит и риск"), KeyboardButton(text="🧾 История")],
        [KeyboardButton(text="🪙 Монеты"), KeyboardButton(text="ℹ️ Помощь")],
    ],
    resize_keyboard=True,
)


def coins_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    muted = store.muted(chat_id)
    buttons = [
        InlineKeyboardButton(
            text=f"{'❌' if s in muted else '✅'} {s.removesuffix('USDT')}", callback_data=f"coin:{s}"
        )
        for s in SYMBOLS
    ]
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([
        InlineKeyboardButton(text="Все включить", callback_data="coins:on"),
        InlineKeyboardButton(text="Все выключить", callback_data="coins:off"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


COINS_TEXT = "<b>Монеты для сигналов</b>\n✅ — присылать, ❌ — не присылать. Нажмите на монету, чтобы переключить."


@router.message(F.text == "🪙 Монеты")
@router.message(Command("coins"))
async def coins_command(message: Message) -> None:
    store.subscribe(message.chat.id)
    await message.answer(COINS_TEXT, reply_markup=coins_keyboard(message.chat.id))


@router.callback_query(F.data.startswith(("coin:", "coins:")))
async def coins_callback(query: CallbackQuery) -> None:
    kind, _, value = (query.data or "").partition(":")
    chat_id = query.from_user.id
    if kind == "coin" and value in SYMBOLS:
        store.toggle_coin(chat_id, value)
    elif kind == "coins":
        store.set_all_coins(chat_id, value == "on")
    await query.answer()
    if query.message:
        await query.message.edit_reply_markup(reply_markup=coins_keyboard(chat_id))


def settings_text(chat_id: int) -> str:
    deposit, risk_pct = store.get_settings(chat_id)
    prefs = store.get_prefs(chat_id)
    limit = prefs["loss_limit"]
    limit_text = f"{limit:g}R (≈ ${limit * deposit * risk_pct / 100:.2f})" if limit > 0 else "выключен"
    week = prefs["week_limit"]
    week_text = f"{week:g}R (≈ ${week * deposit * risk_pct / 100:.2f})" if week > 0 else "выключен"
    quiet_text = (
        f"{prefs['quiet_start']:02d}:00–{prefs['quiet_end']:02d}:00" if prefs["quiet_start"] >= 0 else "выключены"
    )
    return (
        "<b>Настройки</b>\n"
        f"Депозит: <b>${deposit:g}</b>\n"
        f"Риск на сделку: <b>{risk_pct:g}%</b>\n"
        f"Дневной лимит убытка: <b>{limit_text}</b>\n"
        f"Недельный лимит убытка: <b>{week_text}</b>\n"
        f"Тихие часы: <b>{quiet_text}</b> (сообщения приходят без звука)\n"
        f"Часовой пояс: <b>UTC{prefs['tz']:+d}</b>\n\n"
        "Ряды кнопок: депозит, риск, дневной лимит, недельный лимит, тихие часы.\n"
        "Команды: /deposit 75 · /risk 1 · /limit 2 · /weeklimit 4 · /quiet 23 7 · /tz 3"
    )


async def refresh_settings(query: CallbackQuery) -> None:
    if query.message:
        try:
            await query.message.edit_text(settings_text(query.from_user.id), reply_markup=settings_keyboard(query.from_user.id))
        except TelegramAPIError:
            pass


@router.callback_query(F.data.startswith(("lim:", "wlim:", "quiet:")))
async def prefs_callback(query: CallbackQuery) -> None:
    kind, _, raw = (query.data or "").partition(":")
    if kind == "lim":
        store.set_pref(query.from_user.id, loss_limit=float(raw))
    elif kind == "wlim":
        store.set_pref(query.from_user.id, week_limit=float(raw))
    elif raw == "toggle":
        if store.get_prefs(query.from_user.id)["quiet_start"] >= 0:
            store.set_pref(query.from_user.id, quiet_start=-1, quiet_end=-1)
        else:
            store.set_pref(query.from_user.id, quiet_start=23, quiet_end=7)
    else:
        start, end = (int(x) for x in raw.split("-"))
        store.set_pref(query.from_user.id, quiet_start=start, quiet_end=end)
    await query.answer("Сохранено")
    await refresh_settings(query)


@router.callback_query(F.data.startswith(("take:", "skip:")))
async def trade_callback(query: CallbackQuery) -> None:
    action, _, signal_id = (query.data or "").partition(":")
    state = 1 if action == "take" else 0
    store.mark_trade(query.from_user.id, signal_id, state)
    await query.answer("Записал: вы вошли в сделку" if state else "Записал: сигнал пропущен")
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=trade_keyboard(signal_id, state))
        except TelegramAPIError:
            pass


def settings_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    deposit, risk_pct = store.get_settings(chat_id)
    prefs = store.get_prefs(chat_id)

    def mark(active: bool, text: str) -> str:
        return f"✔ {text}" if active else text

    deposits = [
        InlineKeyboardButton(text=mark(deposit == v, f"${v}"), callback_data=f"dep:{v}")
        for v in (20, 50, 100, 500)
    ]
    risks = [
        InlineKeyboardButton(text=mark(risk_pct == v, f"{v}%"), callback_data=f"risk:{v}")
        for v in (0.5, 1, 1.5, 2)
    ]
    limits = [
        InlineKeyboardButton(
            text=mark(prefs["loss_limit"] == v, f"Лимит {v}R" if v else "Без лимита"),
            callback_data=f"lim:{v}",
        )
        for v in (1, 2, 3, 0)
    ]
    week_limits = [
        InlineKeyboardButton(
            text=mark(prefs["week_limit"] == v, f"Неделя {v}R" if v else "Без недельного"),
            callback_data=f"wlim:{v}",
        )
        for v in (3, 4, 6, 0)
    ]
    quiet_on = prefs["quiet_start"] >= 0
    current = (prefs["quiet_start"], prefs["quiet_end"])
    quiet = [
        InlineKeyboardButton(
            text="🔕 Тихие: вкл" if quiet_on else "🔔 Тихие: выкл", callback_data="quiet:toggle"
        ),
        InlineKeyboardButton(text=mark(current == (23, 7), "23–07"), callback_data="quiet:23-7"),
        InlineKeyboardButton(text=mark(current == (0, 8), "00–08"), callback_data="quiet:0-8"),
    ]
    return InlineKeyboardMarkup(inline_keyboard=[deposits, risks, limits, week_limits, quiet])


@router.message(F.text == "📊 Статус")
async def menu_status(message: Message) -> None:
    await status_command(message)


@router.message(F.text == "🧾 История")
async def menu_history(message: Message) -> None:
    await history_command(message)


@router.message(F.text == "📈 Статистика")
async def menu_stats(message: Message) -> None:
    await stats_command(message)


@router.message(F.text == "ℹ️ Помощь")
async def menu_help(message: Message) -> None:
    await message.answer(HELP)


@router.message(F.text == "💰 Депозит и риск")
async def menu_settings(message: Message) -> None:
    await message.answer(settings_text(message.chat.id), reply_markup=settings_keyboard(message.chat.id))


@router.callback_query(F.data.startswith(("dep:", "risk:")))
async def settings_callback(query: CallbackQuery) -> None:
    kind, _, raw = (query.data or "").partition(":")
    store.set_setting(query.from_user.id, "deposit" if kind == "dep" else "risk_pct", float(raw))
    await query.answer("Сохранено")
    await refresh_settings(query)


@router.message(CommandStart())
async def start_command(message: Message) -> None:
    if message.chat.type != "private":
        await message.answer("Откройте бота в личном чате и отправьте /start.")
        return
    store.subscribe(message.chat.id)
    deposit, risk_pct = store.get_settings(message.chat.id)
    await message.answer(
        f"Вы подписаны. Депозит ${deposit:g}, риск {risk_pct:g}% на сделку.\n"
        "Измените кнопкой «💰 Депозит и риск».\n\n" + HELP,
        reply_markup=MENU,
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


@router.message(Command("limit"))
async def limit_command(message: Message, command: CommandObject) -> None:
    value = _parse_number(command)
    if value is None or not 0 <= value <= 20:
        await message.answer("Пример: /limit 2 — стоп на день после убытка 2R. /limit 0 — без лимита.")
        return
    store.set_pref(message.chat.id, loss_limit=value)
    await message.answer(settings_text(message.chat.id))


@router.message(Command("weeklimit"))
async def week_limit_command(message: Message, command: CommandObject) -> None:
    value = _parse_number(command)
    if value is None or not 0 <= value <= 50:
        await message.answer("Пример: /weeklimit 4 — пауза до понедельника после убытка 4R за неделю. 0 — без лимита.")
        return
    store.set_pref(message.chat.id, week_limit=value)
    await message.answer(settings_text(message.chat.id))


@router.message(Command("quiet"))
async def quiet_command(message: Message, command: CommandObject) -> None:
    arg = (command.args or "").strip().lower()
    if arg == "off":
        store.set_pref(message.chat.id, quiet_start=-1, quiet_end=-1)
        await message.answer(settings_text(message.chat.id))
        return
    try:
        start, end = (int(x) for x in arg.split())
    except ValueError:
        start = end = -1
    if not (0 <= start <= 23 and 0 <= end <= 23 and start != end):
        await message.answer("Пример: /quiet 23 7 — с 23:00 до 07:00 без звука. /quiet off — выключить.")
        return
    store.set_pref(message.chat.id, quiet_start=start, quiet_end=end)
    await message.answer(settings_text(message.chat.id))


@router.message(Command("tz"))
async def tz_command(message: Message, command: CommandObject) -> None:
    value = _parse_number(command)
    if value is None or value != int(value) or not -12 <= value <= 14:
        await message.answer("Пример: /tz 3 — для Москвы (UTC+3).")
        return
    store.set_pref(message.chat.id, tz=int(value))
    await message.answer(settings_text(message.chat.id))


@router.message(Command("report"))
async def report_command(message: Message) -> None:
    await message.answer(format_period_report(message.chat.id, 1)[0])


@router.message(Command("week"))
async def week_command(message: Message) -> None:
    await message.answer(format_period_report(message.chat.id, 7)[0])


def personal_stats(chat_id: int) -> str:
    mine = my_results(chat_id)
    if not mine:
        return "\n\n<i>Ваших отмеченных сделок пока нет — нажимайте «Я вошёл» под сигналом.</i>"
    return (
        f"\n\n<b>Ваши сделки («Я вошёл»):</b> {len(mine)} · в плюс {sum(1 for r in mine if r > 0)} · "
        f"итог {sum(mine):+.1f}R"
    )


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
        + personal_stats(message.chat.id)
    )


STATUS_ICONS = {"COMPLETED": "✅ тейк", "STOPPED": "❌ стоп", "EXPIRED": "⏹ время"}


@router.message(Command("history"))
async def history_command(message: Message) -> None:
    rows = store.recent(10)
    if not rows:
        await message.answer("Закрытых сигналов пока нет.")
        return
    lines = ["<b>Последние сигналы</b>"]
    for symbol, side, status, result, closed_at in rows:
        lines.append(
            f"{closed_at[:16]} · {side} {symbol.removesuffix('USDT')} · "
            f"{STATUS_ICONS.get(status, status)} · {result:+.2f}R"
        )
    await message.answer("\n".join(lines))


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
    lines.append(feed_status())
    await message.answer("\n".join(lines))


async def run() -> None:
    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не задан BOT_TOKEN. Укажите его в файле .env как BOT_TOKEN=ваш_токен.")

    async with Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) as bot:
        engine.event_handler = lambda event: handle_market_event(bot, event)
        background = None
        try:
            await start_market_data()
            LAST["tick"] = time.monotonic()
            background = asyncio.create_task(background_loop(bot))

            open_now = [s for s in engine.signals if s["status"] == "ACTIVE"]

            async def started(_: int) -> str:
                text = "♻️ <b>Бот запущен</b> и получает данные Bybit."
                if open_now:
                    text += (
                        f"\nВосстановлено открытых сигналов: {len(open_now)} "
                        "(" + ", ".join(s["symbol"].removesuffix("USDT") for s in open_now) + "). "
                        "Слежение за ними продолжается."
                    )
                return text

            await notify_subscribers(bot, started)
            logger.info("Telegram bot started. Send /start to subscribe.")
            await dispatcher.start_polling(bot)
        finally:
            if background:
                background.cancel()
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
