"""Стратегия «откат по тренду» на 4h. Параметры подобраны бэктестом (research.py)."""
from typing import Any

import pandas as pd
import pandas_ta_classic as ta

TIMEFRAME = 240
SL_ATR = 2.0
TP_R = 1.0
MAX_BARS = 48
RSI_LOW = 35
RSI_HIGH = 65
MIN_CANDLES = 210


DAILY_EMA = 50


def daily_trend_ok(side: str, daily_closes: list[float]) -> bool:
    """Сигнал по тренду 4h берём, только если закрытие последнего полного дня по ту же сторону от EMA50 на 1d."""
    if len(daily_closes) < DAILY_EMA + 10:
        return True
    closes = pd.Series(daily_closes, dtype=float)
    ema = ta.ema(closes, length=DAILY_EMA).iloc[-1]
    if pd.isna(ema):
        return True
    return closes.iloc[-1] > ema if side == "LONG" else closes.iloc[-1] < ema


def pullback_signal(frame: pd.DataFrame) -> dict[str, Any] | None:
    """frame: закрытые свечи по возрастанию времени, последняя строка — только что закрытая."""
    if len(frame) < MIN_CANDLES:
        return None
    close = frame["close"]
    ema50 = ta.ema(close, length=50)
    ema200 = ta.ema(close, length=200)
    rsi = ta.rsi(close, length=14)
    atr = ta.atr(frame["high"], frame["low"], close, length=14)
    values = (ema50.iloc[-1], ema200.iloc[-1], rsi.iloc[-1], rsi.iloc[-2], atr.iloc[-1])
    if any(pd.isna(v) for v in values):
        return None

    price = float(close.iloc[-1])
    up = price > ema200.iloc[-1] and ema50.iloc[-1] > ema200.iloc[-1]
    down = price < ema200.iloc[-1] and ema50.iloc[-1] < ema200.iloc[-1]
    if up and rsi.iloc[-2] < RSI_LOW <= rsi.iloc[-1]:
        side = "LONG"
    elif down and rsi.iloc[-2] > RSI_HIGH >= rsi.iloc[-1]:
        side = "SHORT"
    else:
        return None

    risk = SL_ATR * float(atr.iloc[-1])
    sign = 1 if side == "LONG" else -1
    return {
        "side": side,
        "entry": price,
        "stop_loss": price - sign * risk,
        "take_profit": price + sign * TP_R * risk,
        "risk": risk,
        "risk_pct": risk / price * 100,
        "rsi": float(rsi.iloc[-1]),
        "atr": float(atr.iloc[-1]),
        "ema50": float(ema50.iloc[-1]),
        "ema200": float(ema200.iloc[-1]),
    }


WATCH_LONG = 40
WATCH_SHORT = 60


def watch_signal(frame: pd.DataFrame) -> dict[str, Any] | None:
    """Предупреждение: цена в тренде, RSI только что вошёл в зону перед сигналом."""
    if len(frame) < MIN_CANDLES:
        return None
    close = frame["close"]
    ema50 = ta.ema(close, length=50).iloc[-1]
    ema200 = ta.ema(close, length=200).iloc[-1]
    rsi = ta.rsi(close, length=14)
    now, prev = rsi.iloc[-1], rsi.iloc[-2]
    if any(pd.isna(v) for v in (ema50, ema200, now, prev)):
        return None
    price = float(close.iloc[-1])
    if price > ema200 and ema50 > ema200 and prev >= WATCH_LONG > now:
        return {"side": "LONG", "price": price, "rsi": float(now), "trigger": RSI_LOW}
    if price < ema200 and ema50 < ema200 and prev <= WATCH_SHORT < now:
        return {"side": "SHORT", "price": price, "rsi": float(now), "trigger": RSI_HIGH}
    return None
