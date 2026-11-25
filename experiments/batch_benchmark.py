"""Measure fixed four-task workloads on the same EIA captures in Python and Airflow."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter, sleep
from uuid import uuid4

import psycopg
import requests

from experiments.airflow import Airflow
from experiments.benchmark import (
    AIRFLOW_VERSION,
    DUAL_RUNNERS,
    _airflow_manifest_path,
    _digest,
    _ensure_benchmark_database,
    _ensure_slots,
    _prepare_database,
    _runtime_settings,
    _timing_error,
    _update_digest,
    _verify_database,
    _write_result,
)
from renewflow.batch import (
    BATCH_DAG,
    BATCH_TASKS,
    START_DATE,
    BatchReplay,
    batch_snapshots,
    normalize_batch_snapshot,
    read_batch_replay,
)
from renewflow.db import connect_observations

POLL_INTERVAL_SECONDS = 0.5
OUTPUT_CHECKSUMS = ("observation_sha256", "hourly_sha256", "daily_sha256")
SUBREGIONAL_CHECKSUMS = ("subregional_sha256", "daily_subregional_sha256")


def prepare_input(replay: BatchReplay, days: int) -> dict:
    captures = {}
    observation_hash = hashlib.sha256()
    subregional_hash = hashlib.sha256()
    observations_by_authority = {}
    zone_count = 0
    for authority, manifest in sorted(replay.manifests.items()):
        recorded = json.loads(manifest.read_text(encoding="utf-8"))["captures"]
        subregions = replay.subregions.get(authority, ())
        observations_by_authority[authority] = 0
        for day, path in batch_snapshots(manifest, days):
            label = f"{authority}/{day}"
            metadata = recorded[day.isoformat()]
            if hashlib.sha256(path.read_bytes()).hexdigest() != metadata["sha256"]:
                raise ValueError(f"snapshot checksum differs from the manifest for {label}")
            captures[label] = metadata
            observations, subregional = normalize_batch_snapshot(path, day, authority, subregions)
            ordered = observations.sort_values(["hour_start_utc", "metric"])
            _update_digest(observation_hash, ordered.itertuples(index=False, name=None))
            zone_rows = 0
            if subregional is not None:
                _update_digest(subregional_hash, subregional.itertuples(index=False, name=None))
                zone_rows = len(subregional)
            observations_by_authority[authority] += len(observations) + zone_rows
            zone_count += zone_rows
    return {
        "captures": captures,
        "source_rows": sum(capture["source_rows"] for capture in captures.values()),
        "input_bytes": sum(capture["bytes"] for capture in captures.values()),
        "expected_observations": sum(observations_by_authority.values()),
        "observations_by_authority": observations_by_authority,
        "expected_observation_sha256": observation_hash.hexdigest(),
        **({
            "subregions": replay.subregions,
            "expected_subregional_observations": zone_count,
            "expected_subregional_sha256": subregional_hash.hexdigest(),
        } if replay.subregions else {}),
    }


def _verify_subregional_output(schema: str) -> dict:
    with connect_observations(benchmark_schema=schema) as connection, connection.transaction():
        count = connection.execute("SELECT count(*) FROM hourly_subregional_demand").fetchone()[0]
        with connection.cursor(name="subregional_checksum") as cursor:
            cursor.execute(
                'SELECT respondent, hour_start_utc, subregion, value_mwh FROM hourly_subregional_demand '
                'ORDER BY respondent COLLATE "C", hour_start_utc, subregion COLLATE "C"'
            )
            checksum = _digest(cursor)
        daily = connection.execute(
            'SELECT * FROM daily_subregional_demand ORDER BY respondent COLLATE "C", subregion COLLATE "C", utc_date'
        ).fetchall()
    return {
        "subregional_observations": count,
        "subregional_sha256": checksum,
        "daily_subregional_sha256": _digest(daily),
        "partial_subregion_days": sum(row[3] != 24 for row in daily),
    }


def _python_trial(args, trial_id: str, schema: str, days: int) -> tuple[float, bool, list[dict]]:
    command = [
        sys.executable, "-m", "renewflow.batch", "--manifest", str(args.manifest),
        "--days", str(days), "--run-id", trial_id,
    ]
    started = perf_counter()
    process = subprocess.run(
        command, env=os.environ | {"RENEWFLOW_BENCHMARK_SCHEMA": schema},
        capture_output=True, text=True, timeout=args.timeout, check=False,
    )
    elapsed = perf_counter() - started
    events = [
        json.loads(line.removeprefix("renewflow_event "))
        for line in process.stdout.splitlines()
        if line.startswith("renewflow_event ")
    ]
    return elapsed, process.returncode == 0, [event for event in events if event["status"] != "started"]


def _airflow_trial(args, airflow: Airflow, trial_id: str, schema: str, days: int) -> tuple[float, dict, list[dict]]:
    runs_path = f"/dags/{BATCH_DAG}/dagRuns"
    run_id = f"manual__{trial_id}"
    path = f"{runs_path}/{run_id}"
    body = {
        "dag_run_id": run_id,
        "logical_date": None,
        "conf": {"replay_manifest": args.airflow_manifest, "days": days, "benchmark_schema": schema},
    }
    started = perf_counter()
    airflow.call("POST", runs_path, json=body)
    while perf_counter() - started < args.timeout:
        run = airflow.call("GET", path)
        if run["state"] in ("success", "failed", "canceled"):
            break
        sleep(POLL_INTERVAL_SECONDS)
    else:
        raise TimeoutError(f"Airflow run {run_id} did not complete within {args.timeout} seconds")
    elapsed = perf_counter() - started
    events = [{
        "runner": "airflow", "run_id": run_id, "utc_date": None, "stage": row["task_id"],
        "attempt": row["try_number"], "status": row["state"], "started_at": row["start_date"],
        "finished_at": row["end_date"], "duration_seconds": row["duration"],
    } for row in airflow.call("GET", f"{path}/taskInstances")["task_instances"]]
    return elapsed, run, events


def _attempt_metrics(events: list[dict]) -> dict:
    first = [event for event in events if event["attempt"] == 1]
    return {
        "attempted_stages": len(first),
        "first_attempt_successes": sum(event["status"] == "success" for event in first),
        "retries": sum(event["attempt"] > 1 for event in events),
        "stages_without_first_attempt": sorted(set(BATCH_TASKS) - {event["stage"] for event in first}),
    }


def run_trial(
    args, airflow: Airflow, days: int, runner: str, slots: int, inputs: dict,
    authorities: tuple[str, ...], directory: Path, references: dict,
) -> dict:
    trial_id = uuid4().hex
    schema = f"trial_{trial_id}"
    include_subregions = "expected_subregional_sha256" in inputs
    task_authorities = dict(zip(BATCH_TASKS, authorities, strict=True))
    result = {
        "experiment": "authority_batches_subregions" if include_subregions else "authority_batches",
        "trial_id": trial_id, "schema": schema, "runner": runner, "slots": slots,
        "started_at": datetime.now(UTC).isoformat(), "start_date": START_DATE.isoformat(),
        "end_date": (START_DATE + timedelta(days=days)).isoformat(), "days": days,
        "authorities": task_authorities,
        "pilot": directory.name == "pilot", "fault": None, "manifest": str(args.manifest), "input": inputs,
        "airflow_version": AIRFLOW_VERSION if runner == "airflow" else None,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS if runner == "airflow" else None,
        "outcome": "failed",
    }
    if not include_subregions:
        result["observations_per_task"] = days * 72
    try:
        result.update(_runtime_settings(_prepare_database(schema, include_subregions=include_subregions)))
        if runner == "python":
            elapsed, success, events = _python_trial(args, trial_id, schema, days)
        else:
            elapsed, run, events = _airflow_trial(args, airflow, trial_id, schema, days)
            result["airflow_runs"] = [run]
            success = run["state"] == "success"
        for event in events:
            authority = task_authorities.get(event["stage"])
            if authority is not None:
                event["authority"] = authority
                event["expected_observations"] = inputs["observations_by_authority"][authority]
        metrics = _attempt_metrics(events)
        result.update(wall_seconds=elapsed, events=events, metrics=metrics)
        if not success:
            raise ValueError("the runner did not complete successfully")
        if (
            len(events) != len(BATCH_TASKS)
            or metrics["first_attempt_successes"] != len(BATCH_TASKS)
            or metrics["stages_without_first_attempt"]
        ):
            raise ValueError("expected exactly four successful first-attempt authority tasks")
        output = _verify_database(schema)
        result["output"] = output
        if (
            output["days_published"] != len(authorities) * days or output["partial_days"]
            or output["observations"] != len(authorities) * days * 72
            or output["observation_sha256"] != inputs["expected_observation_sha256"]
        ):
            raise ValueError("PostgreSQL rows differ from the expected complete observations")
        checksum_keys = OUTPUT_CHECKSUMS
        if include_subregions:
            subregional = _verify_subregional_output(schema)
            output.update(
                subregional, authority_observations=output["observations"],
                observations=output["observations"] + subregional["subregional_observations"],
            )
            if (
                output["observations"] != inputs["expected_observations"]
                or subregional["subregional_observations"] != inputs["expected_subregional_observations"]
                or subregional["subregional_sha256"] != inputs["expected_subregional_sha256"]
                or subregional["partial_subregion_days"]
            ):
                raise ValueError("PostgreSQL subregional demand differs from the expected complete observations")
            checksum_keys += SUBREGIONAL_CHECKSUMS
        if error := _timing_error(result):
            raise ValueError(error)
        checksums = tuple(output[key] for key in checksum_keys)
        if references.setdefault(days, checksums) != checksums:
            raise ValueError("PostgreSQL observations or analytics differ across configurations")
        result.update(outcome="success", observations_per_second=output["observations"] / elapsed)
    except (KeyError, OSError, RuntimeError, TimeoutError, ValueError, subprocess.TimeoutExpired, psycopg.Error) as error:
        result["error_category"] = type(error).__name__
        if isinstance(error, (RuntimeError, TimeoutError, ValueError)):
            result["error"] = str(error)
    path = directory / f"{trial_id}.json"
    _write_result(path, result)
    print(f"{runner} ({slots} slots) | {days} UTC days per authority | {result['outcome']} | {path}", flush=True)
    return result


def run_suite(args, airflow: Airflow, replay: BatchReplay) -> None:
    inputs = {days: prepare_input(replay, days) for days in replay.cohorts}
    if airflow.call("GET", f"/dags/{BATCH_DAG}")["is_paused"]:
        raise ValueError(f"unpause {BATCH_DAG} in the Airflow UI")
    _ensure_benchmark_database()
    pilot = args.output_dir / "pilot"
    pilot.mkdir()
    references = {}
    authorities = tuple(replay.manifests)
    active_slots = None
    # The three pilot records stay in a separate directory from measured trials.
    plan = [(pilot, 30, DUAL_RUNNERS)]
    for days in replay.cohorts:
        for first in range(len(DUAL_RUNNERS)):
            plan.append((args.output_dir, days, DUAL_RUNNERS[first:] + DUAL_RUNNERS[:first]))
    for directory, days, configurations in plan:
        for runner, slots in configurations:
            if runner == "airflow" and slots != active_slots:
                _ensure_slots(slots)
                active_slots = slots
            result = run_trial(args, airflow, days, runner, slots, inputs[days], authorities, directory, references)
            if result["outcome"] != "success":
                raise ValueError("trial failed verification; review its JSON record before continuing")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=3600, help="timeout per trial in seconds")
    args = parser.parse_args()
    try:
        if args.timeout <= 0:
            raise ValueError("--timeout must be positive")
        if os.environ.get("POSTGRES_HOST", "127.0.0.1") not in ("", "localhost", "127.0.0.1", "::1"):
            raise ValueError("benchmark PostgreSQL must use the local Compose service")
        args.manifest = args.manifest.resolve()
        args.airflow_manifest = _airflow_manifest_path(args.manifest)
        replay = read_batch_replay(args.manifest)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        if any(args.output_dir.iterdir()):
            raise ValueError("use an empty output directory for the authority batch suite")
        with requests.Session() as session:
            run_suite(args, Airflow(session), replay)
    except (KeyError, OSError, TypeError, ValueError, RuntimeError, subprocess.CalledProcessError, psycopg.Error) as error:
        message = str(error) if isinstance(error, (OSError, RuntimeError, ValueError)) else type(error).__name__
        parser.exit(1, f"{message}\n")


if __name__ == "__main__":
    main()
