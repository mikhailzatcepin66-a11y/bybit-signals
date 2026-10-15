"""Картинки для Telegram: график сигнала и кривая результатов. Без matplotlib бот работает без картинок."""
import io
from datetime import datetime, timezone

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
except ImportError:
    plt = None

BG = "#0f172a"
GRID = "#1e293b"
TEXT = "#cbd5e1"
UP = "#22c55e"
DOWN = "#ef4444"


def _style(ax) -> None:
    ax.set_facecolor(BG)
    ax.grid(color=GRID, linewidth=0.6)
    ax.tick_params(colors=TEXT, labelsize=8)
    for spine in ax.spines.values():
        spine.set_color(GRID)
    ax.yaxis.tick_right()


def _png(fig) -> bytes:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=110, facecolor=BG)
    plt.close(fig)
    return buffer.getvalue()


def signal_chart(candles: list, signal: dict) -> bytes | None:
    """candles: объекты с start (мс), open, high, low, close."""
    if plt is None or len(candles) < 5:
        return None
    candles = candles[-60:]
    entry, stop, take = signal["entry"], signal["stop_loss"], signal["take_profit"]
    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor=BG)
    _style(ax)
    for i, c in enumerate(candles):
        color = UP if c.close >= c.open else DOWN
        ax.vlines(i, c.low, c.high, color=color, linewidth=1)
        ax.add_patch(Rectangle((i - 0.35, min(c.open, c.close)), 0.7, max(abs(c.close - c.open), 1e-12), color=color))
    n = len(candles)
    ax.set_xlim(-1, n + 9)
    ax.axhspan(min(entry, take), max(entry, take), color=UP, alpha=0.12)
    ax.axhspan(min(entry, stop), max(entry, stop), color=DOWN, alpha=0.12)
    for price, color, label in ((take, UP, "Take"), (entry, "#38bdf8", "Entry"), (stop, DOWN, "Stop")):
        ax.hlines(price, -1, n, color=color, linewidth=1.2, linestyle="--")
        ax.text(n + 0.5, price, f" {label} {price:g}", color=color, fontsize=8, va="center")
    low = min(min(c.low for c in candles), stop)
    high = max(max(c.high for c in candles), take)
    pad = (high - low) * 0.05
    ax.set_ylim(low - pad, high + pad)
    ticks = list(range(0, n, 10))
    ax.set_xticks(ticks)
    ax.set_xticklabels(
        [datetime.fromtimestamp(candles[i].start / 1000, timezone.utc).strftime("%d.%m") for i in ticks]
    )
    coin = signal["symbol"].removesuffix("USDT")
    ax.set_title(f"{signal['side']} {coin}/USDT - 4h", color=TEXT, fontsize=11)
    fig.tight_layout()
    return _png(fig)


def equity_chart(results: list[float], title: str) -> bytes | None:
    """results: результаты сделок в R по порядку закрытия."""
    if plt is None or not results:
        return None
    equity = [0.0]
    for r in results:
        equity.append(equity[-1] + r)
    fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
    _style(ax)
    color = UP if equity[-1] >= 0 else DOWN
    xs = range(len(equity))
    ax.plot(xs, equity, color=color, linewidth=2)
    ax.fill_between(xs, equity, 0, color=color, alpha=0.15)
    ax.axhline(0, color=TEXT, linewidth=0.8)
    ax.set_xlabel("trades", color=TEXT, fontsize=8)
    ax.set_title(f"{title}: {equity[-1]:+.1f}R", color=TEXT, fontsize=11)
    fig.tight_layout()
    return _png(fig)
