"""Shared date, retry, and attempt logging rules."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from time import perf_counter

from renewflow.eia import SERIES

EXTRACT_RETRIES = 2
RETRY_DELAY_SECONDS = 5


class InjectedExtractFailure(RuntimeError):
    """A transient fault injected during a replay demonstration."""


def observation_date(value: str | None) -> date:
    """Select an explicit completed UTC operating day."""
    try:
        day = date.fromisoformat(value)
        if value != day.isoformat() or day >= datetime.now(UTC).date():
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("observation_date must be a completed UTC date in YYYY-MM-DD format") from None
    return day


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
        raise InjectedExtractFailure("Injected transient extract failure for replay")


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
