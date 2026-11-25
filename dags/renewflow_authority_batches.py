"""Run four independent EIA authority batches against a benchmark schema."""

import logging
from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from renewflow.batch import BATCH_DAG, BATCH_TASKS

log = logging.getLogger(__name__)


@task(retries=0, do_xcom_push=False)
def process_authority(index: int) -> None:
    import psycopg

    from renewflow.batch import load_authority_batch, read_batch_replay
    from renewflow.pipeline import record_attempt
    from renewflow.replay import replay_path

    context = get_current_context()
    run = context["dag_run"]
    conf = run.conf or {}
    try:
        with record_attempt(
            log.info, runner="airflow", run_id=run.run_id, stage=BATCH_TASKS[index],
            attempt=context["ti"].try_number,
        ) as counts:
            replay = read_batch_replay(replay_path(Path("/opt/airflow/replay"), conf["replay_manifest"]))
            days = conf["days"]
            if days not in replay.cohorts:
                raise ValueError("requested days are outside the captured cohorts")
            authority, manifest = tuple(replay.manifests.items())[index]
            counts.update(load_authority_batch(
                manifest, authority, days, schema=conf["benchmark_schema"],
                subregions=replay.subregions.get(authority, ()),
            ))
    except (KeyError, OSError, TypeError, ValueError, psycopg.Error) as error:
        raise AirflowFailException(f"Authority batch failed ({type(error).__name__})") from None


@dag(
    dag_id=BATCH_DAG, schedule=None, start_date=datetime(2025, 1, 15, tzinfo=UTC),
    catchup=False, max_active_runs=1, tags=["renewflow", "benchmark"],
)
def authority_batches() -> None:
    for index, task_id in enumerate(BATCH_TASKS):
        process_authority.override(task_id=task_id)(index)


authority_batches()
