"""Compare sequential replay with manually submitted Airflow backfills."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from collections import defaultdict
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from math import isfinite
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import psycopg
import requests
from psycopg import sql

from experiments import utc_days
from experiments.airflow import Airflow, STAGES, current_attempts
from experiments.capture import snapshot_metadata
from renewflow.db import BENCHMARK_DATABASE, connect_observations
from renewflow.eia import SERIES
from renewflow.pipeline import replay_fault
from renewflow.replay import read_manifest, read_snapshot
from renewflow.transform import normalize_series

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
DUAL_RUNNERS = (("python", 1), ("airflow", 1), ("airflow", 4))
TIERS = (1, 7, 30, 365, 730)
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


def _ensure_benchmark_database() -> None:
    with connect_observations() as connection:
        exists = connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (BENCHMARK_DATABASE,))
        if exists.fetchone() is None:
            with suppress(psycopg.errors.DuplicateDatabase):
                connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(BENCHMARK_DATABASE)))


def _prepare_database(schema: str, *, include_subregions: bool = False) -> str:
    with connect_observations(benchmark_schema=schema) as connection, connection.transaction():
        version = connection.execute("SHOW server_version").fetchone()[0]
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        for filename in ("schema.sql", "analytics.sql"):
            connection.execute((SQL_DIR / filename).read_text(encoding="utf-8"))
        if include_subregions:
            connection.execute((SQL_DIR / "subregional_demand.sql").read_text(encoding="utf-8"))
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


def _airflow_manifest_path(manifest: Path) -> str:
    root = Path(os.environ.get("RENEWFLOW_REPLAY_DIR") or "tests/fixtures").resolve()
    manifest = manifest.resolve()
    try:
        return manifest.relative_to(root).as_posix()
    except ValueError:
        raise ValueError(
            f"Airflow replay mount {root} does not contain {manifest}. "
            f"Set RENEWFLOW_REPLAY_DIR={manifest.parent} in .env and run "
            "docker compose --profile airflow up -d --wait."
        ) from None


def _airflow_trial(args, airflow: Airflow, trial_id: str, schema: str, days: list[date], slots: int):
    conf = {"replay_manifest": args.airflow_manifest, "benchmark_schema": schema, "benchmark_trial_id": trial_id}
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
        **_runtime_settings(postgres_version),
        "airflow_version": AIRFLOW_VERSION if runner == "airflow" else None,
        "airflow_max_active_runs": slots if runner == "airflow" else None,
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
            if error := _timing_error(result):
                raise ValueError(error)
            result["outcome"] = "success"
            result["observations_per_second"] = actual["observations"] / elapsed
    except (KeyError, RuntimeError, TimeoutError, subprocess.TimeoutExpired, psycopg.Error, ValueError) as error:
        result["error_category"] = type(error).__name__
        if isinstance(error, (RuntimeError, TimeoutError, ValueError)):
            result["error"] = str(error)
    path = args.output_dir / f"{trial_id}.json"
    _write_result(path, result)
    print(f"{runner} ({slots} slots) | {len(dates)} UTC days | {result['outcome']} | {path}", flush=True)
    return result


def _resume_trials(args, days: list[date], available: dict, inputs_cache: dict, settings: dict) -> dict:
    retained = defaultdict(list)
    imported = []
    excluded = {}
    for path in sorted(args.resume_from.glob("*.json")):
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            excluded[path.name] = "trial JSON is incomplete or invalid"
            continue
        if "trial_id" not in result:
            continue
        if result["outcome"] != "success" or result["fault"]:
            excluded[path.name] = "trial did not complete cleanly or used a controlled fault"
            continue
        size = result["days"]
        runner, slots = result["runner"], result["slots"]
        subset = days[:size]
        if size not in TIERS or (runner, slots) not in DUAL_RUNNERS or len(subset) != size:
            excluded[path.name] = "workload or runner configuration falls outside the requested suite"
            continue
        end = (subset[-1] + timedelta(days=1)).isoformat()
        expected_dates = (subset[0].isoformat(), end)
        if (result["start_date"], result["end_date"]) != expected_dates or not set(subset) <= available.keys():
            excluded[path.name] = "dates or snapshot coverage differ from the requested suite"
            continue
        if any(result.get(key) != value for key, value in settings.items()) or (
            runner == "airflow" and (
                result.get("airflow_version") != AIRFLOW_VERSION or result.get("airflow_max_active_runs") != slots
                or result.get("poll_interval_seconds") != 0.5
            )
        ):
            excluded[path.name] = "recorded host or software settings differ"
            continue
        if error := _timing_error(result):
            excluded[path.name] = error
            continue
        if size not in inputs_cache:
            inputs_cache[size] = prepare_input(args.manifest, subset)
        output = result["output"]
        if (
            result["input"] != inputs_cache[size]
            or output["observation_sha256"] != inputs_cache[size]["expected_observation_sha256"]
            or output["days_published"] != size or output["partial_days"]
        ):
            excluded[path.name] = "snapshot checksums or committed observations differ"
            continue
        if runner == "airflow":
            result["events"] = current_attempts(result["events"], result["airflow_runs"])
        result["metrics"] = attempt_metrics(result["events"], subset)
        if result["metrics"]["stages_without_first_attempt"]:
            excluded[path.name] = "trial is missing task attempt evidence"
            continue
        group = retained[size, runner, slots]
        if len(group) == (5 if size <= 30 else 3):
            excluded[path.name] = "repetition target already met"
            continue
        result.setdefault("resumed_from", str(path.resolve()))
        _write_result(args.output_dir / path.name, result)
        group.append(result)
        imported.append(path.name)
    _write_result(args.output_dir / "resume.json", {"source": str(args.resume_from), "reused": imported, "excluded": excluded})
    print(f"Reused {len(imported)} verified trials. See {args.output_dir / 'resume.json'}", flush=True)
    return retained


def _suite(args, airflow: Airflow, days: list[date]) -> None:
    available = read_manifest(args.manifest)
    inputs_cache = {}
    retained = {}
    if args.resume_from:
        with connect_observations() as connection:
            settings = _runtime_settings(connection.execute("SHOW server_version").fetchone()[0])
        retained = _resume_trials(args, days, available, inputs_cache, settings)
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
        inputs = inputs_cache.get(size) or prepare_input(args.manifest, subset)
        for index in range(repeats):
            order = DUAL_RUNNERS[index % 3:] + DUAL_RUNNERS[:index % 3]
            for runner, slots in order:
                completed = retained.get((size, runner, slots), [])
                if index < len(completed):
                    result = completed[index]
                else:
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
    parser.add_argument("--resume-from", type=Path, help="reuse verified results from an earlier suite directory")
    parser.add_argument("--timeout", type=int, default=3600, help="seconds per timed trial")
    parser.add_argument("--long-tier-budget", type=int, default=7200, help="projected seconds per long tier")
    parser.add_argument("--fail-first", help="run a separate transient fault trial at YYYY-MM-DD:metric")
    args = parser.parse_args()
    if (os.environ.get("POSTGRES_HOST") or "127.0.0.1") not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("benchmark PostgreSQL host must point to the local Compose database")
    try:
        days = utc_days(args.start_date, args.end_date)
        fault = replay_fault(args.fail_first, replay=True)
        if fault and (args.runner == "suite" or fault[0] not in days):
            raise ValueError("fault trials require one runner and a date inside the requested range")
        if args.timeout <= 0 or args.long_tier_budget <= 0:
            raise ValueError("time limits must be positive")
        if args.resume_from and (
            args.runner != "suite" or not args.resume_from.is_dir()
            or args.resume_from.resolve() == args.output_dir.resolve() or any(args.output_dir.glob("*.json"))
        ):
            raise ValueError("resume requires a suite, an existing source directory, and a fresh results directory")
        if args.runner in {"airflow", "suite"}:
            args.airflow_manifest = _airflow_manifest_path(args.manifest)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        _ensure_benchmark_database()
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
        parser.exit(1, "Could not start or verify the local Airflow scheduler.\n")
    except psycopg.Error:
        parser.exit(1, "Could not prepare the PostgreSQL benchmark database.\n")
    except (KeyError, OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
