"""Run four extract, transform, and load chains against a benchmark schema."""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from renewflow.batch import (
    AUTHORITY_COUNT, BATCH_DAG, LOAD_RETRIES, LOAD_RETRY_DELAY, authority_batch, execute_stage, read_batch_replay,
)
from renewflow.replay import replay_path

log = logging.getLogger(__name__)


def _run_stage(index: int, stage: str, source: str | None):
    context = get_current_context()
    conf = context["dag_run"].conf or {}
    try:
        replay = read_batch_replay(replay_path(Path("/opt/airflow/replay"), conf["replay_manifest"]))
        if not replay.csv_sources:
            raise ValueError("the authority benchmark requires a direct CSV manifest")
        batch = authority_batch(
            replay, index, conf["days"], conf["benchmark_schema"], conf.get("inject_load_failure", False),
        )
        return execute_stage(batch, stage, source, runner="airflow", attempt=context["ti"].try_number, emit=log.info)
    except (KeyError, OSError, TypeError, ValueError, psycopg.Error) as error:
        detail = str(error) if isinstance(error, ValueError) and not isinstance(error, psycopg.Error) else type(error).__name__
        raise AirflowFailException(f"{stage} failed: {detail}") from None


@task(retries=0)
def extract(index: int) -> str:
    return _run_stage(index, "extract", None)


@task(retries=0)
def transform(index: int, source: str) -> str:
    return _run_stage(index, "transform", source)


@task(retries=LOAD_RETRIES, retry_delay=timedelta(seconds=LOAD_RETRY_DELAY), do_xcom_push=False)
def load(index: int, source: str) -> None:
    _run_stage(index, "load", source)


@dag(
    dag_id=BATCH_DAG, schedule=None, start_date=datetime(2025, 1, 15, tzinfo=UTC),
    catchup=False, max_active_runs=1, tags=["renewflow", "benchmark"],
)
def authority_batches() -> None:
    for index in range(AUTHORITY_COUNT):
        raw = extract.override(task_id=f"extract_authority_{index}")(index)
        normalized = transform.override(task_id=f"transform_authority_{index}")(index, raw)
        load.override(task_id=f"load_authority_{index}")(index, normalized)


dag_instance = authority_batches()
