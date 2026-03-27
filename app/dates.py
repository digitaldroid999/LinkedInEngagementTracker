"""Date parsing and comparison helpers."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from dateutil import parser as date_parser


def _to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_datetime(value: Any) -> datetime | None:
    """Parse a value to naive UTC datetime; preserves time-of-day when present."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return _to_naive_utc(value)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError, OverflowError):
            return None
    s = str(value).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        d = date.fromisoformat(s)
        return datetime.combine(d, time.min)
    if re.match(r"^\d{10,13}$", s):
        try:
            ts = float(s)
            if len(s) >= 13:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError, OverflowError):
            return None
    try:
        return _to_naive_utc(date_parser.parse(s))
    except (ValueError, TypeError, OverflowError):
        return None


def days_ago(n: int) -> date:
    return (datetime.now() - timedelta(days=n)).date()


def format_sheet_datetime(dt: datetime) -> str:
    """ISO 8601 datetime string for Sheets (includes time-of-day, not date-only)."""
    dt = _to_naive_utc(dt)
    return dt.replace(microsecond=0).isoformat(timespec="seconds")


def format_sheet_date(dt: datetime) -> str:
    """ISO date YYYY-MM-DD for Sheets Engagement Date / Scrape Date columns."""
    return _to_naive_utc(dt).date().isoformat()
