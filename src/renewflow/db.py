"""Store validated RenewFlow observations in PostgreSQL."""

import os
import re

import pandas as pd
import psycopg

BENCHMARK_DATABASE = "renewflow_benchmark"

_KEY = ["respondent", "hour_start_utc", "metric"]
_UPSERT = """
    INSERT INTO hourly_observations (respondent, hour_start_utc, metric, value_mwh)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (respondent, hour_start_utc, metric) DO UPDATE
    SET value_mwh = EXCLUDED.value_mwh
    WHERE hourly_observations.value_mwh IS DISTINCT FROM EXCLUDED.value_mwh
"""
_UPSERT_SUBREGIONAL = """
    INSERT INTO hourly_subregional_demand (respondent, hour_start_utc, subregion, value_mwh)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (respondent, hour_start_utc, subregion) DO UPDATE
    SET value_mwh = EXCLUDED.value_mwh
    WHERE hourly_subregional_demand.value_mwh IS DISTINCT FROM EXCLUDED.value_mwh
"""


def connect_observations(*, benchmark_schema: str | None = None) -> psycopg.Connection:
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("set POSTGRES_PASSWORD in the runtime environment")
    database = os.environ.get("POSTGRES_DB") or "renewflow"
    options = {}
    if benchmark_schema is not None:
        if not re.fullmatch(r"trial_[0-9a-f]{32}", benchmark_schema):
            raise ValueError("benchmark schema must be trial_ followed by 32 hexadecimal characters")
        database = BENCHMARK_DATABASE
        options["options"] = f"-c search_path={benchmark_schema}"
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST") or "127.0.0.1",
        port=os.environ.get("POSTGRES_PORT") or "5432",
        dbname=database,
        user=os.environ.get("POSTGRES_USER") or "renewflow",
        password=password,
        connect_timeout=5,
        autocommit=True,
        **options,
    )


def _day_rows(observations: pd.DataFrame, keys: list[str]):
    # Visit keys in the same order when more than one run writes the same day.
    batch = observations[keys + ["value_mwh"]].sort_values(keys)
    return (
        (respondent, hour_start.to_pydatetime(), series, value_mwh)
        for respondent, hour_start, series, value_mwh in batch.itertuples(index=False, name=None)
    )


def load_day(
    connection: psycopg.Connection, observations: pd.DataFrame, *, subregional: pd.DataFrame | None = None
) -> int:
    """Upsert one validated day and return the number of inserted or updated rows."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(_UPSERT, _day_rows(observations, _KEY))
        changed_rows = cursor.rowcount
        if subregional is not None:
            cursor.executemany(_UPSERT_SUBREGIONAL, _day_rows(subregional, ["respondent", "hour_start_utc", "subregion"]))
            changed_rows += cursor.rowcount
    return changed_rows
