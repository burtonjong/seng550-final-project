"""Fetch, validate, and publish one complete CISO operating day."""

import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import CronDataIntervalTimetable, dag, get_current_context, task

from renewflow.eia import RESPONDENT, EIAError, RetryableEIAError, fetch_series
from renewflow.pipeline import (
    EXTRACT_RETRIES,
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
        day = observation_date(run.run_type, run.data_interval_end, conf.get("observation_date"))
    except ValueError as error:
        raise AirflowFailException(str(error)) from None
    return day, run.run_id, context["ti"].try_number, conf


@task(retries=EXTRACT_RETRIES, retry_delay=timedelta(seconds=RETRY_DELAY_SECONDS))
def extract_and_validate(metric: str) -> dict:
    import requests

    from renewflow.transform import normalize_series

    day, run_id, attempt, conf = _run_context()
    try:
        with record_attempt(
            log.info, runner="airflow", run_id=run_id, stage=f"extract_{metric}",
            day=day, attempt=attempt,
        ) as counts:
            manifest = conf.get("replay_manifest")
            if manifest is not None and (not isinstance(manifest, str) or not manifest):
                raise ValueError("replay_manifest must be a relative manifest filename")
            fault = replay_fault(conf.get("fail_first"), replay=manifest is not None)
            fail_on_first_attempt(fault, day, metric, attempt)
            if manifest is not None:
                path = replay_path(Path("/opt/airflow/replay"), manifest)
                snapshots = read_manifest(path)
                if day not in snapshots:
                    raise ValueError(f"replay manifest has no snapshot for {day}")
                rows = read_series(snapshots[day], day, metric)
            else:
                api_key = os.environ.get("EIA_API_KEY", "").strip()
                with requests.Session() as session:
                    rows = fetch_series(session, metric, day, api_key=api_key).rows
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
    except RetryableEIAError:
        raise
    except (EIAError, OSError, ValueError) as error:
        raise AirflowFailException(str(error)) from None


@task(task_id="load_day", retries=0, do_xcom_push=False)
def publish_day(solar: dict, wind: dict, demand: dict) -> None:
    import pandas as pd
    import psycopg

    from renewflow.db import connect_observations, load_day

    day, run_id, attempt, _ = _run_context()
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
                counts["rows_changed"] = load_day(connection, batch)
    except psycopg.Error as error:
        code = f" (SQLSTATE {error.sqlstate})" if error.sqlstate else ""
        raise AirflowFailException(f"PostgreSQL load failed for {day} UTC{code}") from None
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


@dag(
    dag_id="renewflow_daily",
    schedule=CronDataIntervalTimetable("0 19 * * *", timezone="UTC"),
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
