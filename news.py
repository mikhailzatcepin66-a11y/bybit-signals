"""Календарь крупных событий, рядом с которыми рынок часто двигается резко и непредсказуемо.

Даты решений ФРС США (FOMC) на 2026 год взяты с federalreserve.gov. Свои события можно дописать в EVENTS.
Данные по CPI и другим релизам бот не знает, их стоит проверять самостоятельно.
"""
from datetime import datetime, timezone

UTC = timezone.utc
BEFORE_HOURS = 12
AFTER_HOURS = 6

EVENTS: list[tuple[str, datetime]] = [
    ("решение ФРС по ставке", datetime(2026, 1, 28, 19, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 3, 18, 18, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 4, 29, 18, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 6, 17, 18, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 7, 29, 18, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 9, 16, 18, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 10, 28, 18, tzinfo=UTC)),
    ("решение ФРС по ставке", datetime(2026, 12, 9, 19, tzinfo=UTC)),
]


def upcoming_event(now: datetime | None = None) -> dict[str, str] | None:
    """Событие, к которому сейчас стоит быть осторожным (за 12 часов до и 6 часов после)."""
    now = now or datetime.now(UTC)
    for name, when in EVENTS:
        delta_hours = (when - now).total_seconds() / 3600
        if -AFTER_HOURS <= delta_hours <= BEFORE_HOURS:
            return {"name": name, "at": when.isoformat()}
    return None
