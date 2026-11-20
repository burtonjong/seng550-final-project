"""Shared date, retry, and attempt logging rules."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from time import perf_counter

from renewflow.eia import SERIES, RetryableEIAError

EXTRACT_RETRIES = 2
RETRY_DELAY_SECONDS = 5


def observation_date(run_type: str, interval_end: datetime | None, override: str | None) -> date:
    """Map a scheduled/backfill interval or manual date to one UTC operating day."""
    if run_type == "manual":
        if not isinstance(override, str):
            raise ValueError("manual runs require observation_date in YYYY-MM-DD format")
        day = date.fromisoformat(override)
        if override != day.isoformat() or day >= datetime.now(UTC).date():
            raise ValueError("observation_date must be a completed UTC date in YYYY-MM-DD format")
        return day
    if override is not None or interval_end is None or interval_end.utcoffset() is None:
        raise ValueError("scheduled runs and backfills require their own UTC data interval")
    return interval_end.astimezone(UTC).date() - timedelta(days=1)


def replay_fault(value: str | None, *, replay: bool) -> tuple[date, str] | None:
    if value is None:
        return None
    if not replay or not isinstance(value, str):
        raise ValueError("fail_first is available only in replay mode")
    label, _, metric = value.partition(":")
    try:
        day = date.fromisoformat(label)
        if label != day.isoformat() or metric not in SERIES:
            raise ValueError
    except ValueError:
        raise ValueError("fail_first must be YYYY-MM-DD:solar, :wind, or :demand") from None
    return day, metric


def fail_on_first_attempt(
    fault: tuple[date, str] | None, day: date, metric: str, attempt: int
) -> None:
    if fault == (day, metric) and attempt == 1:
        raise RetryableEIAError("Injected transient extract failure for replay")


@contextmanager
def record_attempt(
    emit: Callable[[str], None],
    *,
    runner: str,
    run_id: str,
    stage: str,
    day: date | None = None,
    attempt: int = 1,
) -> Iterator[dict[str, int]]:
    started_at = datetime.now(UTC).isoformat()
    start = perf_counter()
    event = {
        "runner": runner,
        "run_id": run_id,
        "utc_date": day.isoformat() if day else None,
        "stage": stage,
        "attempt": attempt,
        "started_at": started_at,
    }
    emit("renewflow_event " + json.dumps({**event, "status": "started"}))
    counts = {}
    status, category = "success", None
    try:
        yield counts
    except BaseException as error:
        status, category = "failed", type(error).__name__
        raise
    finally:
        emit("renewflow_event " + json.dumps({
            **event,
            **counts,
            "status": status,
            "error_category": category,
            "finished_at": datetime.now(UTC).isoformat(),
            "duration_seconds": perf_counter() - start,
        }))
