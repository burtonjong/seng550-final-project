"""Store validated RenewFlow observations in PostgreSQL."""

import os

import pandas as pd
import psycopg

_KEY = ["respondent", "hour_start_utc", "metric"]
_UPSERT = """
    INSERT INTO hourly_observations (respondent, hour_start_utc, metric, value_mwh)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (respondent, hour_start_utc, metric) DO UPDATE
    SET value_mwh = EXCLUDED.value_mwh
    WHERE hourly_observations.value_mwh IS DISTINCT FROM EXCLUDED.value_mwh
"""


def connect_observations() -> psycopg.Connection:
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("set POSTGRES_PASSWORD in the runtime environment")
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST") or "127.0.0.1",
        port=os.environ.get("POSTGRES_PORT") or "5432",
        dbname=os.environ.get("POSTGRES_DB") or "renewflow",
        user=os.environ.get("POSTGRES_USER") or "renewflow",
        password=password,
        connect_timeout=5,
        autocommit=True,
    )


def load_day(connection: psycopg.Connection, observations: pd.DataFrame) -> int:
    """Upsert one validated day and return the number of inserted or updated rows."""
    # Visit keys in the same order when more than one run writes the same day.
    batch = observations[_KEY + ["value_mwh"]].sort_values(_KEY)
    rows = (
        (respondent, hour_start.to_pydatetime(), metric, value_mwh)
        for respondent, hour_start, metric, value_mwh in batch.itertuples(index=False, name=None)
    )
    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(_UPSERT, rows)
        changed_rows = cursor.rowcount
    return changed_rows
