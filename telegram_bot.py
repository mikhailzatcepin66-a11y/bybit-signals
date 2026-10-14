import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from main import engine, start_market_data, stop_market_data


BASE_DIR = Path(__file__).parent
DATABASE_PATH = BASE_DIR / "subscribers.sqlite3"
logger = logging.getLogger("bybit-telegram")
router = Router()
dispatcher = Dispatcher()
dispatcher.include_router(router)


class SubscriberStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY,
                    subscribed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def subscribe(self, chat_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)",
                (chat_id,),
            )

    def unsubscribe(self, chat_id: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM subscribers WHERE chat_id = ?",
                (chat_id,),
            )

    def list_chat_ids(self) -> list[int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT chat_id FROM subscribers ORDER BY subscribed_at"
            ).fetchall()
        return [int(row[0]) for row in rows]

    def is_subscribed(self, chat_id: int) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM subscribers WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
        return row is not None


subscribers = SubscriberStore(DATABASE_PATH)


def format_price(value: float) -> str:
    return f"{value:,.2f}"


def format_new_signal(signal: dict[str, Any]) -> str:
    side = "🟢 LONG" if signal["side"] == "LONG" else "🔴 SHORT"
    targets = signal["take_profits"]
    return (
        f"<b>{side} · {signal['symbol']} · {signal['interval']}m</b>\n\n"
        f"Вход: <code>{format_price(signal['entry'])}</code>\n"
        f"Stop-loss: <code>{format_price(signal['stop_loss'])}</code>\n"
        f"TP1 (40%): <code>{format_price(targets[0])}</code>\n"
        f"TP2 (40%): <code>{format_price(targets[1])}</code>\n"
        f"TP3 (20%): <code>{format_price(targets[2])}</code>\n\n"
        f"RSI: {signal['rsi']:.1f} · EMA50: {format_price(signal['ema50'])} · "
        f"EMA200: {format_price(signal['ema200'])}\n"
        "После TP1 стоп будет перемещён на цену входа.\n"
        "<i>Информационный сигнал, не финансовая рекомендация.</i>"
    )


def format_signal_update(signal: dict[str, Any]) -> str:
    symbol = signal["symbol"]
    timeframe = signal["interval"]
    targets = signal["take_profits"]
    reached = signal["hit_targets"]
    reached_labels = " · ".join(
        f"TP{index} достигнут ({format_price(targets[index - 1])})"
        for index in reached
    )
    if signal["status"] == "COMPLETED":
        return f"✅ <b>{symbol} · {timeframe}m</b>\nДостигнут TP3. Сигнал завершён."
    if signal["status"] == "BREAKEVEN":
        return (
            f"⚖️ <b>{symbol} · {timeframe}m</b>\n"
            f"{reached_labels}\nЦена вернулась к Entry "
            f"(<code>{format_price(signal['entry'])}</code>): безубыток."
        )
    if signal["status"] == "STOPPED":
        return (
            f"⛔ <b>{symbol} · {timeframe}m</b>\n"
            f"Сработал Stop-loss: <code>{format_price(signal['stop_loss'])}</code>."
        )
    return (
        f"🎯 <b>{symbol} · {timeframe}m</b>\n"
        f"{reached_labels}\n"
        f"Стоп перенесён в безубыток: <code>{format_price(signal['stop_loss'])}</code>."
    )


async def notify_subscribers(bot: Bot, text: str) -> None:
    for chat_id in subscribers.list_chat_ids():
        try:
            await bot.send_message(chat_id, text)
        except TelegramForbiddenError:
            logger.info("Removing blocked or unavailable subscriber chat %s", chat_id)
            subscribers.unsubscribe(chat_id)
        except TelegramAPIError:
            logger.exception("Could not send notification to Telegram chat %s", chat_id)


async def handle_market_event(bot: Bot, event: dict[str, Any]) -> None:
    if event["type"] == "signal":
        await notify_subscribers(bot, format_new_signal(event["signal"]))
    elif event["type"] == "signal_update":
        await notify_subscribers(bot, format_signal_update(event["signal"]))


@router.message(CommandStart())
async def start_command(message: Message) -> None:
    if message.chat.type != "private":
        await message.answer("Пожалуйста, откройте бота в личном чате и отправьте /start.")
        return
    subscribers.subscribe(message.chat.id)
    await message.answer(
        "Вы подписаны на сигналы Bybit для BTCUSDT и ETHUSDT.\n\n"
        "Сигналы приходят после закрытия свечи 5m/15m. Я также сообщу о достижении "
        "TP, переносе стопа в безубыток и завершении сигнала.\n\n"
        "Команды: /status — активные сигналы и цены; /stop — отключить уведомления; "
        "/help — справка.\n\n"
        "<i>Сигналы информационные и не являются финансовой рекомендацией.</i>"
    )


@router.message(Command("stop"))
async def stop_command(message: Message) -> None:
    subscribers.unsubscribe(message.chat.id)
    await message.answer("Уведомления отключены. Чтобы подписаться снова, отправьте /start.")


@router.message(Command("help"))
async def help_command(message: Message) -> None:
    await message.answer(
        "<b>Bybit Signal Bot</b>\n"
        "/start — подписаться на личные сигналы\n"
        "/status — текущие цены и активные сигналы\n"
        "/stop — отключить уведомления\n"
        "/help — эта справка"
    )


@router.message(Command("status"))
async def status_command(message: Message) -> None:
    lines = ["<b>Bybit Signal Desk</b>"]
    for symbol in ("BTCUSDT", "ETHUSDT"):
        price = engine.prices.get(symbol)
        formatted = format_price(price) if price is not None else "ожидание данных"
        lines.append(f"{symbol}: <code>{formatted}</code>")
    active = [signal for signal in engine.signals if signal["status"] == "ACTIVE"]
    if active:
        lines.append("\n<b>Активные сигналы:</b>")
        for signal in active:
            lines.append(
                f"{signal['side']} {signal['symbol']} {signal['interval']}m · "
                f"Entry <code>{format_price(signal['entry'])}</code> · "
                f"SL <code>{format_price(signal['stop_loss'])}</code>"
            )
    else:
        lines.append("\nАктивных сигналов сейчас нет.")
    if message.chat.type == "private" and subscribers.is_subscribed(message.chat.id):
        lines.append("\nВы подписаны на уведомления.")
    await message.answer("\n".join(lines))


async def run() -> None:
    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError(
            "Не задан BOT_TOKEN. Создайте бота через @BotFather и сохраните токен "
            "в локальном файле .env как BOT_TOKEN=ваш_токен."
        )

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
