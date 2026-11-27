import json
import os
from datetime import UTC, date, datetime
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
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


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
    try:
        connection = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
    except psycopg.Error:
        raise pytest.fail.Exception(
            "Could not connect using RENEWFLOW_TEST_DATABASE_URL. "
            "Check the published Compose port, credentials, and renewflow_test database.",
            pytrace=False,
        ) from None
    with connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(isolated_schema))
        try:
            connection.execute(sql.SQL("SET search_path TO {}").format(isolated_schema))
            connection.execute((SQL_DIR / "schema.sql").read_text(encoding="utf-8"))
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
def test_subregional_load_is_idempotent_and_atomic_with_authority_rows(pg_connection, observations):
    pg_connection.execute((SQL_DIR / "subregional_demand.sql").read_text(encoding="utf-8"))
    pg_connection.execute("SET TIME ZONE 'America/Los_Angeles'")
    subregional = observations.loc[
        observations["metric"] == "demand", ["respondent", "hour_start_utc"]
    ].assign(subregion="PGAE", value_mwh=Decimal("1"))
    assert load_day(pg_connection, observations, subregional=subregional) == 96
    assert load_day(pg_connection, observations, subregional=subregional) == 0
    assert pg_connection.execute(
        "SELECT respondent, subregion, utc_date, observed_hours, demand_mwh, peak_hourly_demand_mwh "
        "FROM daily_subregional_demand"
    ).fetchone() == ("CISO", "PGAE", date(2025, 1, 15), 24, Decimal("24"), Decimal("1"))

    original = _stored_rows(pg_connection)
    observations.loc[0, "value_mwh"] = Decimal("1000000")
    subregional.loc[subregional.index[-1], "value_mwh"] = Decimal("-1")
    with pytest.raises(psycopg.errors.CheckViolation):
        load_day(pg_connection, observations, subregional=subregional)
    assert _stored_rows(pg_connection) == original
    assert pg_connection.execute("SELECT sum(value_mwh) FROM hourly_subregional_demand").fetchone() == (Decimal("24"),)


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


@pytest.fixture
def analytics_connection(pg_connection, observations):
    pg_connection.execute("SET TIME ZONE 'America/Los_Angeles'")
    load_day(pg_connection, observations)
    pg_connection.execute((SQL_DIR / "analytics.sql").read_text(encoding="utf-8"))
    return pg_connection


@pytest.mark.postgres
def test_energy_views_match_capture_at_utc_day_boundaries(analytics_connection):
    hourly = analytics_connection.execute(
        """
        SELECT hour_start_utc, solar_mwh, wind_mwh, demand_mwh, solar_wind_mwh,
               solar_wind_to_demand_ratio, demand_minus_solar_wind_mwh
        FROM hourly_energy_summary
        ORDER BY hour_start_utc
        """
    ).fetchall()
    assert len(hourly) == 24
    first = hourly[0]
    assert first[:5] == (datetime(2025, 1, 15, tzinfo=UTC), 9562, 1054, 23631, 10616)
    assert first[5] == pytest.approx(Decimal(10616) / 23631, abs=Decimal("1e-16"))
    assert first[6] == 13015
    assert hourly[-1][0] == datetime(2025, 1, 15, 23, tzinfo=UTC)
    assert min(row[1] for row in hourly) == -56

    daily = analytics_connection.execute(
        """
        SELECT respondent, utc_date, observed_hours, solar_mwh, wind_mwh, demand_mwh,
               solar_wind_mwh, peak_hourly_demand_mwh, solar_wind_to_demand_ratio
        FROM daily_energy_summary
        """
    ).fetchall()
    assert len(daily) == 1
    assert daily[0][:-1] == (
        "CISO", date(2025, 1, 15), 24, 101447, 25448, 589463, 126895, 27956
    )
    assert daily[0][-1] == pytest.approx(Decimal(126895) / 589463, abs=Decimal("1e-16"))


@pytest.mark.postgres
def test_seasonal_profile_counts_only_complete_utc_days(analytics_connection, observations):
    second_day = observations.copy()
    second_day["hour_start_utc"] += pd.Timedelta(days=1)
    second_day.loc[second_day["metric"] == "solar", "value_mwh"] += Decimal(2)
    load_day(analytics_connection, second_day)

    analytics_connection.execute(
        """
        INSERT INTO hourly_observations VALUES
            ('CISO', '2025-01-17 00:00+00', 'solar', 1),
            ('CISO', '2025-01-17 00:00+00', 'wind', 1),
            ('CISO', '2025-01-17 00:00+00', 'demand', 0),
            ('CISO', '2025-01-17 01:00+00', 'solar', 1)
        """
    )
    partial = analytics_connection.execute(
        """
        SELECT observed_hours, solar_wind_to_demand_ratio
        FROM daily_energy_summary
        WHERE utc_date = DATE '2025-01-17'
        """
    ).fetchone()
    assert partial == (1, None)

    profile = analytics_connection.execute(
        (SQL_DIR / "seasonal_profile.sql").read_text(encoding="utf-8")
    ).fetchall()
    assert len(profile) == 24
    assert profile[0] == (
        "CISO", "winter", 0, 2, date(2025, 1, 15), date(2025, 1, 16), 9563, 1054, 23631
    )
    assert [row[2] for row in profile] == list(range(24))
    assert {row[3] for row in profile} == {2}
