"""Database, Compose, and verification helpers for the CSV benchmark."""

import hashlib
import json
import os
import platform
import subprocess
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from math import isfinite
from pathlib import Path

import psycopg
from psycopg import sql

from renewflow.db import BENCHMARK_DATABASE, connect_observations

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
DUAL_RUNNERS = (("python", 1), ("airflow", 1), ("airflow", 4))
COMPOSE = ["docker", "compose", "--profile", "airflow"]
AIRFLOW_VERSION = "3.3.2"


def _field(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        if not value:
            return "0"
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    return value


def _digest(rows) -> str:
    checksum = hashlib.sha256()
    _update_digest(checksum, rows)
    return checksum.hexdigest()


def _update_digest(checksum, rows) -> None:
    for row in rows:
        checksum.update((json.dumps([_field(value) for value in row], default=str) + "\n").encode())


def _ensure_benchmark_database() -> None:
    with connect_observations() as connection:
        exists = connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (BENCHMARK_DATABASE,))
        if exists.fetchone() is None:
            with suppress(psycopg.errors.DuplicateDatabase):
                connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(BENCHMARK_DATABASE)))


def _prepare_database(schema: str) -> str:
    with connect_observations(benchmark_schema=schema) as connection, connection.transaction():
        version = connection.execute("SHOW server_version").fetchone()[0]
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        for filename in ("schema.sql", "analytics.sql", "subregional_demand.sql"):
            connection.execute((SQL_DIR / filename).read_text(encoding="utf-8"))
    return version


def _verify_database(schema: str) -> dict:
    with connect_observations(benchmark_schema=schema) as connection:
        rows = connection.execute(
            "SELECT respondent, hour_start_utc, metric, value_mwh FROM hourly_observations "
            "ORDER BY respondent, hour_start_utc, metric"
        ).fetchall()
        hourly = connection.execute(
            "SELECT * FROM hourly_energy_summary ORDER BY respondent, hour_start_utc"
        ).fetchall()
        daily = connection.execute(
            "SELECT * FROM daily_energy_summary ORDER BY respondent, utc_date"
        ).fetchall()
    return {
        "observations": len(rows), "observation_sha256": _digest(rows),
        "hourly_sha256": _digest(hourly), "daily_sha256": _digest(daily),
        "partial_days": sum(row[2] != 24 for row in daily), "days_published": len(daily),
    }


def _airflow_manifest_path(manifest: Path) -> str:
    root = Path(os.environ.get("RENEWFLOW_REPLAY_DIR") or ".renewflow").resolve()
    manifest = manifest.resolve()
    try:
        return manifest.relative_to(root).as_posix()
    except ValueError:
        raise ValueError(
            f"Airflow replay mount {root} does not contain {manifest}. "
            f"Set RENEWFLOW_REPLAY_DIR={manifest.parent} in .env and run "
            "docker compose --profile airflow up -d --wait."
        ) from None


def _ensure_slots(slots: int) -> None:
    env = os.environ | {"AIRFLOW_PARALLELISM": str(slots)}
    subprocess.run([*COMPOSE, "up", "-d", "--wait", "airflow-scheduler"], env=env, check=True)
    value = subprocess.check_output(
        [*COMPOSE, "exec", "-T", "airflow-scheduler", "printenv", "AIRFLOW__CORE__PARALLELISM"],
        env=env, text=True,
    )
    if value.strip() != str(slots):
        raise RuntimeError("Airflow scheduler has an unexpected task slot count")


def _runtime_settings(postgres_version: str) -> dict:
    return {
        "python_version": platform.python_version(), "postgres_version": postgres_version,
        "uv_lock_sha256": hashlib.sha256((SQL_DIR.parent / "uv.lock").read_bytes()).hexdigest(),
        "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
    }


def _timing_error(result: dict) -> str | None:
    elapsed = result.get("wall_seconds", 0)
    if not isinstance(elapsed, (float, int)) or not isfinite(elapsed) or elapsed <= 0:
        return "trial has no valid measured elapsed time"
    if result["runner"] == "airflow":
        runs = result.get("airflow_runs", [])
        if not runs or any(not run.get("queued_at") or not run.get("end_date") for run in runs):
            return "trial is missing Airflow queue or finish timestamps"
        queued = min(datetime.fromisoformat(run["queued_at"]) for run in runs)
        finished = max(datetime.fromisoformat(run["end_date"]) for run in runs)
        span = (finished - queued).total_seconds()
        # Allow one second for clock precision and small drift between the host and the VM.
        if span > elapsed + 1:
            return f"Airflow recorded {span:.2f}s within a measured trial of {elapsed:.2f}s"
    return None


def _write_result(path: Path, result: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
