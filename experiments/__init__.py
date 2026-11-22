"""Reproducible EIA capture and local benchmark commands."""

from datetime import UTC, date, datetime, timedelta


def utc_days(start: date, end: date) -> list[date]:
    if start >= end or end > datetime.now(UTC).date():
        raise ValueError("choose a nonempty range of completed UTC days")
    return [start + timedelta(days=index) for index in range((end - start).days)]
