"""Shared benchmark attempt logging."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from time import perf_counter


@contextmanager
def record_attempt(
    emit: Callable[[str], None],
    *,
    runner: str,
    run_id: str,
    stage: str,
    attempt: int = 1,
) -> Iterator[dict[str, int]]:
    started_at = datetime.now(UTC).isoformat()
    start = perf_counter()
    event = {
        "runner": runner,
        "run_id": run_id,
        "utc_date": None,
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
