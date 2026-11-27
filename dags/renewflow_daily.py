"""Replay, validate, and publish one complete CISO operating day."""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from renewflow.eia import RESPONDENT
from renewflow.pipeline import (
    EXTRACT_RETRIES,
    InjectedExtractFailure,
    RETRY_DELAY_SECONDS,
    fail_on_first_attempt,
    observation_date,
    record_attempt,
    replay_fault,
)
from renewflow.replay import read_manifest, read_series, replay_path

log = logging.getLogger(__name__)


def _run_context():
    context = get_current_context()
    run = context["dag_run"]
    conf = run.conf or {}
    try:
        day = observation_date(conf.get("observation_date"))
    except ValueError as error:
        raise AirflowFailException(str(error)) from None
    ti = context["ti"]
    # Airflow retains try numbers when a task is cleared and rerun.
    previous_tries = ti.max_tries - (ti.task.retries or 0)
    return day, run.run_id, ti.try_number - previous_tries, conf


@task(retries=EXTRACT_RETRIES, retry_delay=timedelta(seconds=RETRY_DELAY_SECONDS))
def extract_and_validate(metric: str) -> dict:
    from renewflow.transform import normalize_series

    day, run_id, attempt, conf = _run_context()
    try:
        with record_attempt(
            log.info, runner="airflow", run_id=run_id, stage=f"extract_{metric}",
            day=day, attempt=attempt,
        ) as counts:
            manifest = conf.get("replay_manifest")
            if not isinstance(manifest, str) or not manifest:
                raise ValueError("replay_manifest must be a relative manifest filename")
            fault = replay_fault(conf.get("fail_first"))
            fail_on_first_attempt(fault, day, metric, attempt)
            path = replay_path(Path("/opt/airflow/replay"), manifest)
            snapshots = read_manifest(path)
            if day not in snapshots:
                raise ValueError(f"replay manifest has no snapshot for {day}")
            rows = read_series(snapshots[day], day, metric)
            counts["rows_received"] = len(rows)
            result = normalize_series(metric, day, rows)
            counts["rows_validated"] = len(result.observations)
            counts["identical_duplicates"] = result.identical_duplicates
            counts["negative_generation_hours"] = result.negative_generation_hours
            return {
                "metric": metric,
                "hours": [hour.isoformat() for hour in result.observations["hour_start_utc"]],
                "values_mwh": [str(value) for value in result.observations["value_mwh"]],
            }
    except InjectedExtractFailure:
        raise
    except (OSError, ValueError) as error:
        raise AirflowFailException(str(error)) from None


@task(task_id="load_day", retries=0, do_xcom_push=False)
def publish_day(solar: dict, wind: dict, demand: dict) -> None:
    import pandas as pd
    import psycopg

    from renewflow.db import connect_observations, publish_day as publish_application_day

    day, run_id, attempt, conf = _run_context()
    if conf.get("benchmark_schema") is not None:
        raise AirflowFailException("use the authority benchmark DAG for trial schemas")
    try:
        with record_attempt(
            log.info, runner="airflow", run_id=run_id, stage="load_day", day=day, attempt=attempt,
        ) as counts:
            frames = [
                pd.DataFrame({
                    "respondent": RESPONDENT,
                    "hour_start_utc": pd.to_datetime(series["hours"], utc=True),
                    "metric": series["metric"],
                    "value_mwh": [Decimal(value) for value in series["values_mwh"]],
                })
                for series in (solar, wind, demand)
            ]
            batch = pd.concat(frames, ignore_index=True)
            counts["rows_validated"] = len(batch)
            with connect_observations() as connection:
                status, changed = publish_application_day(connection, batch)
                counts["days_skipped"] = int(status == "skipped")
                counts["rows_changed"] = changed
    except psycopg.Error as error:
        code = f" (SQLSTATE {error.sqlstate})" if error.sqlstate else ""
        raise AirflowFailException(f"PostgreSQL load failed for {day} UTC{code}") from None
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


@dag(
    dag_id="renewflow_daily",
    schedule=None,
    start_date=datetime(2019, 1, 1, 19, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["renewflow", "eia"],
)
def renewflow_daily():
    solar = extract_and_validate.override(task_id="extract_solar")("solar")
    wind = extract_and_validate.override(task_id="extract_wind")("wind")
    demand = extract_and_validate.override(task_id="extract_demand")("demand")
    publish_day(solar, wind, demand)


renewflow_daily()
