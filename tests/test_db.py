import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pandas as pd
import psycopg
import pytest
from psycopg import sql

from renewflow.db import load_day
from renewflow.transform import normalize_series

FIXTURE = Path(__file__).parent / "fixtures" / "eia-ciso-2025-01-15.json"
SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


@pytest.fixture
def observations():
    capture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    day = date.fromisoformat(capture["utc_date"])
    frames = [
        normalize_series(metric, day, series["response"]["data"]).observations
        for metric, series in capture["series"].items()
    ]
    return pd.concat(frames, ignore_index=True)


def test_loader_sends_ordered_native_utc_values_to_psycopg(observations):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.rowcount = 72
    rows = []
    cursor.executemany.side_effect = lambda _query, batch: rows.extend(batch)

    assert load_day(connection, observations) == 72

    connection.transaction.assert_called_once()
    assert len(rows) == 72
    assert [row[:3] for row in rows] == sorted(row[:3] for row in rows)
    assert all(type(row[1]) is datetime and row[1].isoformat().endswith("+00:00") for row in rows)
    assert all(isinstance(row[3], Decimal) for row in rows)


@pytest.fixture
def pg_connection():
    dsn = os.environ.get("RENEWFLOW_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("set RENEWFLOW_TEST_DATABASE_URL to run PostgreSQL integration tests")
    isolated_schema = sql.Identifier(f"renewflow_test_{uuid4().hex}")
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(isolated_schema))
        try:
            connection.execute(sql.SQL("SET search_path TO {}").format(isolated_schema))
            connection.execute(SCHEMA.read_text(encoding="utf-8"))
            yield connection
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(isolated_schema))


def _stored_rows(connection):
    return connection.execute(
        "SELECT respondent, hour_start_utc, metric, value_mwh FROM hourly_observations "
        "ORDER BY respondent, hour_start_utc, metric"
    ).fetchall()


@pytest.mark.postgres
def test_postgres_load_is_idempotent_and_applies_a_revision(pg_connection, observations):
    assert load_day(pg_connection, observations) == 72
    original = _stored_rows(pg_connection)
    assert len(original) == 72
    assert load_day(pg_connection, observations) == 0
    assert _stored_rows(pg_connection) == original

    observations.loc[0, "value_mwh"] = Decimal("123.456789012345678901")
    assert load_day(pg_connection, observations) == 1
    updated = _stored_rows(pg_connection)
    assert len(updated) == 72
    changed = [after for before, after in zip(original, updated, strict=True) if before != after]
    assert len(changed) == 1
    assert changed[0][-1] == Decimal("123.456789012345678901")


@pytest.mark.postgres
@pytest.mark.parametrize("already_loaded", [False, True])
def test_postgres_rolls_back_all_writes_on_error(pg_connection, observations, already_loaded):
    if already_loaded:
        load_day(pg_connection, observations)
    original = _stored_rows(pg_connection)

    observations.loc[0, "value_mwh"] = Decimal("1000000")
    last_hour = observations["hour_start_utc"].max()
    observations.loc[len(observations)] = ("CISO", last_hour, "zz-invalid", Decimal("10"))

    with pytest.raises(psycopg.errors.CheckViolation):
        load_day(pg_connection, observations)

    assert _stored_rows(pg_connection) == original
