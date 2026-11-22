"""Compare sequential replay with manually submitted Airflow backfills."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import psycopg
import requests
from psycopg import sql

from experiments import utc_days
from experiments.airflow import Airflow, STAGES
from experiments.capture import snapshot_metadata
from renewflow.db import connect_observations
from renewflow.eia import SERIES
from renewflow.pipeline import replay_fault
from renewflow.replay import read_manifest, read_snapshot
from renewflow.transform import normalize_series

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
DUAL_RUNNERS = (("python", 1), ("airflow", 1), ("airflow", 4))
TIERS = (1, 7, 30, 365, 730)
COMPOSE = ["docker", "compose", "--env-file", "/dev/null", "-p", "renewflow-benchmark", "--profile", "airflow"]


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
    for row in rows:
        checksum.update((json.dumps([_field(value) for value in row], default=str) + "\n").encode())
    return checksum.hexdigest()


def prepare_input(manifest: Path, days: list[date]) -> dict:
    snapshots = read_manifest(manifest)
    recorded = json.loads(manifest.read_text(encoding="utf-8")).get("captures", {})
    start_rows = []
    captures = {}
    for day in days:
        if day not in snapshots:
            raise ValueError(f"replay manifest has no snapshot for {day}")
        path = snapshots[day]
        metadata = snapshot_metadata(path, day)
        if day.isoformat() in recorded and metadata["sha256"] != recorded[day.isoformat()]["sha256"]:
            raise ValueError(f"snapshot checksum differs from the manifest for {day}")
        captures[day.isoformat()] = metadata
        _, rows = read_snapshot(path)
        for metric in SERIES:
            frame = normalize_series(metric, day, rows[metric]).observations
            start_rows.extend(frame.itertuples(index=False, name=None))
    expected = _digest(sorted(start_rows, key=lambda row: row[:3]))
    return {
        "expected_observation_sha256": expected,
        "source_rows": sum(item["source_rows"] for item in captures.values()),
        "input_bytes": sum(item["bytes"] for item in captures.values()),
        "captures": captures,
    }


def _prepare_database(schema: str) -> str:
    with connect_observations() as connection:
        if connection.info.dbname != "renewflow_benchmark":
            raise ValueError("run benchmarks against the dedicated renewflow_benchmark database")
        version = connection.execute("SHOW server_version").fetchone()[0]
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    with connect_observations(benchmark_schema=schema) as connection, connection.transaction():
        for filename in ("schema.sql", "analytics.sql"):
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


def _python_trial(args, trial_id: str, schema: str, start: date, end: date) -> tuple[float, bool, list[dict]]:
    command = [
        sys.executable, "-m", "renewflow.cli", "--replay-manifest", str(args.manifest),
        "--start-date", start.isoformat(), "--end-date", end.isoformat(), "--run-id", trial_id,
    ]
    if args.fail_first:
        command.extend(["--fail-first", args.fail_first])
    started = perf_counter()
    result = subprocess.run(
        command, env=os.environ | {"RENEWFLOW_BENCHMARK_SCHEMA": schema},
        capture_output=True, text=True, timeout=args.timeout, check=False,
    )
    elapsed = perf_counter() - started
    events = [json.loads(line.removeprefix("renewflow_event ")) for line in result.stdout.splitlines()
              if line.startswith("renewflow_event ")]
    return elapsed, result.returncode == 0, events


def _airflow_trial(args, airflow: Airflow, trial_id: str, schema: str, days: list[date], slots: int):
    root = Path(os.environ.get("RENEWFLOW_REPLAY_DIR") or "tests/fixtures").resolve()
    try:
        manifest = args.manifest.resolve().relative_to(root).as_posix()
    except ValueError:
        raise ValueError("the benchmark manifest must be under RENEWFLOW_REPLAY_DIR") from None
    conf = {"replay_manifest": manifest, "benchmark_schema": schema, "benchmark_trial_id": trial_id}
    if args.fail_first:
        conf["fail_first"] = args.fail_first
    body = airflow.prepare(days, slots, conf)
    started = perf_counter()
    backfill_id = airflow.submit(body, args.timeout)
    elapsed = perf_counter() - started
    runs = airflow.runs(body, trial_id)
    observed = sorted(datetime.fromisoformat(run["logical_date"]).astimezone(UTC).date() for run in runs)
    success = observed == days and all(run["state"] == "success" for run in runs)
    return elapsed, success, airflow.attempts(runs), backfill_id, runs


def attempt_metrics(events: list[dict], days: list[date]) -> dict:
    attempts = [event for event in events if event["stage"] in STAGES and event["status"] != "started"]
    first = [event for event in attempts if event["attempt"] == 1]
    retries = sum(event["attempt"] > 1 for event in attempts)
    missing = sorted({(day.isoformat(), stage) for day in days for stage in STAGES}
                     - {(event["utc_date"], event["stage"]) for event in first})
    failures = [event for event in attempts if event["status"] != "success" and event.get("finished_at")]
    recovery = None
    repeated_stages = None
    if failures:
        failure = min(failures, key=lambda event: datetime.fromisoformat(event["finished_at"]))
        finished = datetime.fromisoformat(failure["finished_at"])
        published = [datetime.fromisoformat(event["finished_at"]) for event in attempts
                     if event["utc_date"] == failure["utc_date"] and event["stage"] == "load_day"
                     and event["status"] == "success" and event.get("finished_at")]
        if published and max(published) >= finished:
            recovery = (max(published) - finished).total_seconds()
            repeated_stages = len({event["stage"] for event in attempts
                                   if event["utc_date"] == failure["utc_date"] and event["attempt"] > 1})
    return {
        "attempted_stages": len(first), "first_attempt_successes": sum(e["status"] == "success" for e in first),
        "retries": retries, "stages_without_first_attempt": missing,
        "recovery_seconds": recovery, "recovery_repeated_stages": repeated_stages,
        "recovery_required_operator": False if recovery is not None else None,
    }


def _ensure_slots(slots: int) -> None:
    env = os.environ | {"AIRFLOW_PARALLELISM": str(slots), "EIA_API_KEY": ""}
    subprocess.run([*COMPOSE, "up", "-d", "--wait"], env=env, check=True)
    for service, key, expected in (
        ("postgres", "POSTGRES_DB", "renewflow_benchmark"),
        ("airflow-scheduler", "POSTGRES_DB", "renewflow_benchmark"),
        ("airflow-scheduler", "AIRFLOW__CORE__PARALLELISM", str(slots)),
    ):
        value = subprocess.check_output([*COMPOSE, "exec", "-T", service, "printenv", key], env=env, text=True)
        if value.strip() != expected:
            raise RuntimeError(f"{service} has an unexpected {key}")


def trial(args, airflow: Airflow, dates: list[date], runner: str, slots: int, inputs: dict) -> dict:
    trial_id = uuid4().hex
    schema = f"trial_{trial_id}"
    started = datetime.now(UTC).isoformat()
    postgres_version = _prepare_database(schema)
    result = {
        "trial_id": trial_id, "schema": schema, "runner": runner, "slots": slots,
        "started_at": started, "start_date": dates[0].isoformat(),
        "end_date": (dates[-1] + timedelta(days=1)).isoformat(), "days": len(dates),
        "fault": args.fail_first, "manifest": str(args.manifest), "input": inputs,
        "python_version": platform.python_version(), "postgres_version": postgres_version,
        "airflow_version": "3.3.2" if runner == "airflow" else None,
        "airflow_max_active_runs": slots if runner == "airflow" else None,
        "uv_lock_sha256": hashlib.sha256((SQL_DIR.parent / "uv.lock").read_bytes()).hexdigest(),
        "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
        "poll_interval_seconds": 0.5 if runner == "airflow" else None,
        "outcome": "failed",
    }
    try:
        if runner == "python":
            elapsed, success, events = _python_trial(args, trial_id, schema, dates[0], dates[-1] + timedelta(days=1))
        else:
            elapsed, success, events, backfill_id, runs = _airflow_trial(args, airflow, trial_id, schema, dates, slots)
            result["backfill_id"] = backfill_id
            result["airflow_runs"] = runs
        result["wall_seconds"] = elapsed
        result["events"] = events
        result["metrics"] = attempt_metrics(events, dates)
        actual = _verify_database(schema)
        result["output"] = actual
        equivalent = actual["observation_sha256"] == inputs["expected_observation_sha256"] and not actual["partial_days"]
        metrics = result["metrics"]
        evidence = not metrics["stages_without_first_attempt"]
        if args.fail_first:
            evidence = evidence and metrics["recovery_seconds"] is not None and metrics["retries"] > 0
        if success and equivalent and evidence:
            result["outcome"] = "success"
            result["observations_per_second"] = actual["observations"] / elapsed
    except (KeyError, RuntimeError, TimeoutError, subprocess.TimeoutExpired, psycopg.Error, ValueError) as error:
        result["error_category"] = type(error).__name__
        if isinstance(error, (RuntimeError, TimeoutError, ValueError)):
            result["error"] = str(error)
    path = args.output_dir / f"{trial_id}.json"
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"{runner} ({slots} slots) | {len(dates)} UTC days | {result['outcome']} | {path}", flush=True)
    return result


def _suite(args, airflow: Airflow, days: list[date]) -> None:
    available = read_manifest(args.manifest)
    estimates = []
    skipped = {}
    expected_summaries = {}
    slots_active = None
    for size in TIERS:
        subset = days[:size]
        if len(subset) < size or any(day not in available for day in subset):
            skipped[size] = "requested range or complete snapshot coverage is too short"
            continue
        repeats = 5 if size <= 30 else 3
        if size > 30 and (
            not estimates or max(seconds / tested_days for tested_days, seconds in estimates)
            * size * repeats * len(DUAL_RUNNERS) > args.long_tier_budget
        ):
            skipped[size] = "projected clean trials exceed the time budget for this tier"
            continue
        inputs = prepare_input(args.manifest, subset)
        for index in range(repeats):
            order = DUAL_RUNNERS[index % 3:] + DUAL_RUNNERS[:index % 3]
            for runner, slots in order:
                if runner == "airflow" and slots != slots_active:
                    _ensure_slots(slots)
                    slots_active = slots
                result = trial(args, airflow, subset, runner, slots, inputs)
                if result["outcome"] != "success":
                    raise RuntimeError("trial failed verification; review its JSON record before continuing")
                values = tuple(result["output"][key] for key in ("hourly_sha256", "daily_sha256"))
                if expected_summaries.setdefault(size, values) != values:
                    raise RuntimeError("hourly or daily output differs between trials")
                if size <= 30:
                    estimates.append((size, result["wall_seconds"]))
    (args.output_dir / "suite-coverage.json").write_text(json.dumps({"skipped_tiers": skipped}, indent=2) + "\n")
    if not expected_summaries:
        raise RuntimeError("no workload tier had enough complete snapshots")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", choices=("python", "airflow", "suite"), required=True)
    parser.add_argument("--slots", type=int, choices=(1, 4), default=4, help="Airflow task/run slots")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=3600, help="seconds per timed trial")
    parser.add_argument("--long-tier-budget", type=int, default=7200, help="projected seconds per long tier")
    parser.add_argument("--fail-first", help="run a separate transient fault trial at YYYY-MM-DD:metric")
    args = parser.parse_args()
    if os.environ.get("POSTGRES_DB") != "renewflow_benchmark":
        parser.error("set POSTGRES_DB=renewflow_benchmark before benchmarking")
    try:
        days = utc_days(args.start_date, args.end_date)
        fault = replay_fault(args.fail_first, replay=True)
        if fault and (args.runner == "suite" or fault[0] not in days):
            raise ValueError("fault trials require one runner and a date inside the requested range")
        if args.timeout <= 0 or args.long_tier_budget <= 0:
            raise ValueError("time limits must be positive")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        with requests.Session() as session:
            airflow = Airflow(session)
            if args.runner == "suite":
                _suite(args, airflow, days)
            else:
                inputs = prepare_input(args.manifest, days)
                if args.runner == "airflow":
                    _ensure_slots(args.slots)
                slots = args.slots if args.runner == "airflow" else 1
                if trial(args, airflow, days, args.runner, slots, inputs)["outcome"] != "success":
                    parser.exit(1, "Trial failed verification. See the JSON record.\n")
    except subprocess.CalledProcessError:
        parser.exit(1, "Could not start or verify the dedicated Docker Compose benchmark stack.\n")
    except psycopg.Error:
        parser.exit(1, "Could not prepare the PostgreSQL benchmark database.\n")
    except (KeyError, OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
