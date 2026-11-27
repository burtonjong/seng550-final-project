"""Store validated RenewFlow observations in PostgreSQL."""

import hashlib
import json
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


def _digest(rows) -> str:
    # Strip fractional padding without rounding Decimal values.
    canonical = []
    for respondent, hour, code, value in rows:
        number = format(value, "f") if value else "0"
        if "." in number:
            number = number.rstrip("0").rstrip(".")
        canonical.append((respondent, hour.isoformat(), code, number))
    return hashlib.sha256(json.dumps(canonical).encode()).hexdigest()


def publish_day(
    connection: psycopg.Connection, observations: pd.DataFrame, *, subregional: pd.DataFrame | None = None
) -> tuple[str, int]:
    """Resume application loads at the last committed authority-day."""
    authority = observations["respondent"].iat[0]
    day = observations["hour_start_utc"].iat[0].date()
    observation_digest = _digest(_day_rows(observations, _KEY))
    full_digest = None
    if subregional is not None:
        zone_digest = _digest(_day_rows(subregional, ["respondent", "hour_start_utc", "subregion"]))
        full_digest = observation_digest + zone_digest

    with connection.transaction():
        # Both the full import and the one-day replay take this lock.
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s), %s)", (authority, day.toordinal())
        )
        previous = connection.execute(
            """SELECT observation_digest, full_digest FROM renewflow_completed_days
               WHERE respondent = %s AND utc_date = %s""",
            (authority, day),
        ).fetchone()
        if previous == (observation_digest, full_digest):
            return "skipped", 0
        if full_digest is None and previous and previous[0] == observation_digest:
            return "skipped", 0

        changed = load_day(connection, observations, subregional=subregional)
        connection.execute(
            """INSERT INTO renewflow_completed_days VALUES (%s, %s, %s, %s)
               ON CONFLICT (respondent, utc_date) DO UPDATE
               SET observation_digest = EXCLUDED.observation_digest, full_digest = EXCLUDED.full_digest""",
            (authority, day, observation_digest, full_digest),
        )
    return ("updated" if previous else "committed"), changed
