"""Compare staged ETL, concurrency, and recovery on local EIA-930 CSVs."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import perf_counter, sleep
from uuid import uuid4

import psycopg
import requests

from experiments import CSV_EXPERIMENT, STAGED_EXPERIMENT, input_identity
from experiments.airflow import Airflow
from experiments.benchmark import (
    AIRFLOW_VERSION,
    COMPOSE,
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
    FAULT_AFTER_DAYS,
    STAGES,
    START_DATE,
    BatchReplay,
    batch_snapshots,
    normalize_batch_rows,
    normalize_batch_snapshot,
    read_attempts,
    read_batch_replay,
    trial_work,
)
from renewflow.db import connect_observations
from renewflow.eia930_csv import collect_rows, csv_paths, day_payload, verify_sources
from renewflow.replay import snapshot_rows

POLL_INTERVAL_SECONDS = 0.5
OUTPUT_CHECKSUMS = ("observation_sha256", "hourly_sha256", "daily_sha256")
SUBREGIONAL_CHECKSUMS = ("subregional_sha256", "daily_subregional_sha256")
LOAD_FAULT = "CISO load after three committed days"


def prepare_input(replay: BatchReplay, days: int) -> dict:
    dates = replay.selected_dates(days)
    captures = {}
    observation_hash = hashlib.sha256()
    subregional_hash = hashlib.sha256()
    observations_by_authority = {}
    zone_count = 0
    records = None
    if replay.csv_sources:
        records = collect_rows(
            csv_paths(replay.csv_sources), dates[0], dates[-1] + timedelta(days=1),
            zones=replay.subregions, dates=set(dates),
        )
    for authority in sorted(replay.authorities):
        subregions = replay.subregions.get(authority, ())
        observations_by_authority[authority] = 0
        if records is None:
            manifest = replay.manifests[authority]
            recorded = json.loads(manifest.read_text(encoding="utf-8"))["captures"]
            snapshots = dict(batch_snapshots(manifest, dates))
        for day in dates:
            if records is None:
                path = snapshots[day]
                metadata = recorded[day.isoformat()]
                if hashlib.sha256(path.read_bytes()).hexdigest() != metadata["sha256"]:
                    raise ValueError(f"snapshot checksum differs from the manifest for {authority}/{day}")
                captures[f"{authority}/{day}"] = metadata
                observations, subregional = normalize_batch_snapshot(path, day, authority, subregions)
            else:
                payload = day_payload(day, authority, records.get((authority, day), {}))
                rows = snapshot_rows(payload, include_subregions=True)
                observations, subregional = normalize_batch_rows(rows, day, authority, subregions)
            ordered = observations.sort_values(["hour_start_utc", "metric"])
            _update_digest(observation_hash, ordered.itertuples(index=False, name=None))
            zone_rows = 0
            if subregional is not None:
                _update_digest(subregional_hash, subregional.itertuples(index=False, name=None))
                zone_rows = len(subregional)
            observations_by_authority[authority] += len(observations) + zone_rows
            zone_count += zone_rows
    if replay.csv_sources:
        unique_bytes = sum(source["bytes"] for source in replay.csv_sources)
        source_metadata = {
            "source_files": list(replay.csv_sources), "distinct_csv_bytes": unique_bytes,
            "input_bytes": unique_bytes * len(replay.authorities),
        }
    else:
        source_metadata = {
            "captures": captures, "source_rows": sum(item["source_rows"] for item in captures.values()),
            "input_bytes": sum(item["bytes"] for item in captures.values()),
        }
    return {
        "utc_dates": [day.isoformat() for day in dates],
        **source_metadata,
        "expected_observations": sum(observations_by_authority.values()),
        "observations_by_authority": observations_by_authority,
        "expected_observation_sha256": observation_hash.hexdigest(),
        **({
            "subregions": replay.subregions,
            "expected_subregional_observations": zone_count,
            "expected_subregional_sha256": subregional_hash.hexdigest(),
        } if replay.subregions else {}),
    }


def verify_runner_sources(replay: BatchReplay, manifest: Path, airflow_manifest: str) -> None:
    """Verify the host and the scheduler see the same inputs before timing."""
    verify_sources(replay.csv_sources)
    payload = {
        "sources": replay.csv_sources, "manifest": airflow_manifest,
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
    }
    check_mounts = """
import hashlib
import json
import sys
from pathlib import Path

from renewflow.eia930_csv import verify_sources
from renewflow.replay import replay_path

inputs = json.load(sys.stdin)
manifest = replay_path(Path("/opt/airflow/replay"), inputs["manifest"])
if hashlib.sha256(manifest.read_bytes()).hexdigest() != inputs["manifest_sha256"]:
    raise SystemExit("Airflow's benchmark manifest differs from the local manifest")
verify_sources(tuple(inputs["sources"]))
"""
    subprocess.run(
        COMPOSE + ["exec", "-T", "airflow-scheduler", "python", "-c", check_mounts],
        input=json.dumps(payload), text=True, check=True,
    )


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


def _python_trial(args, trial_id: str, schema: str, days: int, fault: bool) -> tuple[float, bool]:
    command = [
        sys.executable, "-m", "renewflow.batch", "--manifest", str(args.manifest),
        "--days", str(days), "--run-id", trial_id,
    ]
    if fault:
        command.append("--inject-load-failure")
    started = perf_counter()
    process = subprocess.run(
        command, env=os.environ | {"RENEWFLOW_BENCHMARK_SCHEMA": schema},
        capture_output=True, text=True, timeout=args.timeout, check=False,
    )
    elapsed = perf_counter() - started
    return elapsed, process.returncode == 0


def _airflow_trial(args, airflow: Airflow, trial_id: str, schema: str, days: int, fault: bool) -> tuple[float, dict, list[dict]]:
    runs_path = f"/dags/{BATCH_DAG}/dagRuns"
    run_id = f"manual__{trial_id}"
    path = f"{runs_path}/{run_id}"
    body = {
        "dag_run_id": run_id,
        "logical_date": None,
        "conf": {
            "replay_manifest": args.airflow_manifest, "days": days, "benchmark_schema": schema,
            "inject_load_failure": fault,
        },
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
    tasks = airflow.call("GET", f"{path}/taskInstances")["task_instances"]
    return elapsed, run, tasks


def _attempt_metrics(events: list[dict], launched_at: datetime) -> dict:
    first = [event for event in events if event["attempt"] == 1]
    times = []
    starts = []
    for event in events:
        start, end = (datetime.fromisoformat(event[key]) for key in ("started_at", "finished_at"))
        if end < start:
            raise ValueError("a stage finished before it started")
        starts.append(start)
        if end > start:
            times.extend(((start, 1), (end, -1)))
    active = peak = 0
    overlap = 0.0
    previous = launched_at
    for timestamp, change in sorted(times):
        if active > 1:
            overlap += (timestamp - previous).total_seconds()
        active += change
        peak = max(peak, active)
        previous = timestamp
    return {
        "attempted_stages": len(first),
        "first_attempt_successes": sum(event["status"] == "success" for event in first),
        "retries": sum(event["attempt"] > 1 for event in events),
        "stages_without_first_attempt": sorted(set(BATCH_TASKS) - {event["stage"] for event in first}),
        "first_stage_delay_seconds": max(0.0, (min(starts) - launched_at).total_seconds()) if starts else None,
        "peak_concurrent_stages": peak,
        "stage_overlap_seconds": overlap,
    }


def _verify_attempts(events: list[dict], fault: bool, days: int, inputs: dict) -> dict:
    by_attempt = {(event["stage"], event["attempt"]): event for event in events}
    target = "load_authority_0"
    expected = {(stage, 1): "failed" if fault and stage == target else "success" for stage in BATCH_TASKS}
    if fault:
        expected[target, 2] = "success"
    if len(events) != len(expected) or {key: event["status"] for key, event in by_attempt.items()} != expected:
        raise ValueError("stage outcomes do not match the expected clean or recovery run")
    for event in events:
        if event["status"] != "success":
            continue
        expected_rows = inputs["observations_by_authority"][event["authority"]]
        if event["days_processed"] != days or (
            not event["stage"].startswith("extract_") and event["rows_processed"] != expected_rows
        ):
            raise ValueError(f"{event['stage']} did not process the expected workload")
    if not fault:
        return {}
    failed, recovered = by_attempt[target, 1], by_attempt[target, 2]
    changed = inputs["observations_by_authority"]["CISO"]
    if (
        failed["error_category"] != "InjectedLoadFailure"
        or failed["days_committed"] != FAULT_AFTER_DAYS or recovered["days_committed"] != days
        or failed["rows_changed"] != changed * FAULT_AFTER_DAYS // days
        or failed["rows_changed"] + recovered["rows_changed"] != changed
    ):
        raise ValueError("recovery evidence does not show a partial commit and idempotent replay")
    recovery = (datetime.fromisoformat(recovered["finished_at"]) - datetime.fromisoformat(failed["finished_at"])).total_seconds()
    if recovery < 0:
        raise ValueError("load recovery finished before the injected failure")
    return {
        "recovery_seconds": recovery, "recovery_repeated_stages": 1,
        "recovery_repeated_days": failed["days_committed"], "recovery_repeated_rows": failed["rows_processed"],
        "recovery_required_operator": False,
    }


def run_trial(
    args, airflow: Airflow, days: int, runner: str, slots: int, inputs: dict,
    authorities: tuple[str, ...], directory: Path, references: dict, *, fault: bool = False,
) -> dict:
    trial_id = uuid4().hex
    schema = f"trial_{trial_id}"
    work = trial_work(schema)
    dates = inputs.get("utc_dates")
    first = date.fromisoformat(dates[0]) if dates else START_DATE
    last = date.fromisoformat(dates[-1]) if dates else first + timedelta(days=days - 1)
    result = {
        "experiment": CSV_EXPERIMENT if "source_files" in inputs else STAGED_EXPERIMENT,
        "trial_id": trial_id, "schema": schema, "runner": runner, "slots": slots,
        "started_at": datetime.now(UTC).isoformat(), "start_date": first.isoformat(),
        "end_date": (last + timedelta(days=1)).isoformat(), "days": days,
        "authorities": list(authorities),
        "pilot": directory.name == "pilot", "fault": LOAD_FAULT if fault else None,
        "manifest": str(args.manifest), "input": inputs,
        "airflow_version": AIRFLOW_VERSION if runner == "airflow" else None,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS if runner == "airflow" else None,
        "outcome": "failed",
    }
    launched_at = None
    try:
        work.mkdir(parents=True, exist_ok=False)
        # The host creates this trial directory and the container uses a different UID.
        work.chmod(0o1777)
        result.update(_runtime_settings(_prepare_database(schema, include_subregions=True)))
        launched_at = datetime.now(UTC)
        result["launched_at"] = launched_at.isoformat()
        if runner == "python":
            elapsed, success = _python_trial(args, trial_id, schema, days, fault)
        else:
            elapsed, run, tasks = _airflow_trial(args, airflow, trial_id, schema, days, fault)
            result["airflow_runs"] = [run]
            result["airflow_tasks"] = tasks
            success = run["state"] == "success"
        events = read_attempts(work)
        task_authorities = dict(zip(BATCH_TASKS, (authority for authority in authorities for _ in STAGES), strict=True))
        for event in events:
            if event["authority"] != task_authorities.get(event["stage"]):
                raise ValueError("a recorded stage belongs to a different authority")
            event["expected_observations"] = inputs["observations_by_authority"][event["authority"]]
        metrics = _attempt_metrics(events, launched_at)
        result.update(wall_seconds=elapsed, events=events, metrics=metrics)
        if not success:
            raise ValueError("the runner did not complete successfully")
        metrics.update(_verify_attempts(events, fault, days, inputs))
        if "source_files" in inputs:
            scanned = sum(event.get("csv_bytes_scanned", 0) for event in events)
            if scanned != inputs["input_bytes"]:
                raise ValueError("recorded CSV scan bytes differ from the benchmark workload")
        if metrics["peak_concurrent_stages"] > slots:
            raise ValueError("recorded concurrent stages exceeded the configured slot count")
        if runner == "airflow":
            expected_tries = {stage: 2 if fault and stage == "load_authority_0" else 1 for stage in BATCH_TASKS}
            if len(tasks) != len(BATCH_TASKS) or {task["task_id"]: task["try_number"] for task in tasks
                                                    if task["state"] == "success"} != expected_tries:
                raise ValueError("Airflow task states or try counts differ from the expected workload")
        output = _verify_database(schema)
        result["output"] = output
        if (
            output["days_published"] != len(authorities) * days or output["partial_days"]
            or output["observations"] != len(authorities) * days * 72
            or output["observation_sha256"] != inputs["expected_observation_sha256"]
        ):
            raise ValueError("PostgreSQL rows differ from the expected complete observations")
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
        if error := _timing_error(result):
            raise ValueError(error)
        checksums = tuple(output[key] for key in OUTPUT_CHECKSUMS + SUBREGIONAL_CHECKSUMS)
        if references.setdefault(days, checksums) != checksums:
            raise ValueError("PostgreSQL observations or analytics differ across configurations")
        result.update(outcome="success", observations_per_second=output["observations"] / elapsed)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.TimeoutExpired, psycopg.Error) as error:
        if launched_at and "events" not in result:
            with suppress(OSError, ValueError):
                result["events"] = read_attempts(work)
        result["error_category"] = type(error).__name__
        if isinstance(error, (RuntimeError, TimeoutError, ValueError)):
            result["error"] = str(error)
    path = directory / f"{trial_id}.json"
    _write_result(path, result)
    kind = "recovery" if fault else "pilot" if result["pilot"] else "clean"
    print(f"{runner} ({slots} slots) | {days} UTC days per authority | {kind} | {result['outcome']} | {path}", flush=True)
    return result


def run_suite(args, airflow: Airflow, replay: BatchReplay) -> None:
    if 7 not in replay.cohorts or (args.mode != "recovery" and 30 not in replay.cohorts):
        raise ValueError("the benchmark suite requires complete 7-day and 30-day cohorts")
    workloads = (7,) if args.mode == "recovery" else replay.cohorts
    inputs = {days: prepare_input(replay, days) for days in workloads}
    if airflow.call("GET", f"/dags/{BATCH_DAG}")["is_paused"]:
        raise ValueError(f"unpause {BATCH_DAG} in the Airflow UI")
    _ensure_benchmark_database()
    references = {}
    completed_recovery = Counter()
    authorities = replay.authorities
    active_slots = None
    plan = []
    if args.mode != "recovery":
        pilot = args.output_dir / "pilot"
        pilot.mkdir()
        plan.append((pilot, 30, DUAL_RUNNERS, False))
        for days in workloads:
            for first in range(len(DUAL_RUNNERS)):
                plan.append((args.output_dir, days, DUAL_RUNNERS[first:] + DUAL_RUNNERS[:first], False))
    else:
        for path in args.output_dir.glob("*.json"):
            previous = json.loads(path.read_bytes())
            if previous.get("experiment") != CSV_EXPERIMENT:
                raise ValueError("use the staged benchmark's clean results directory for recovery")
            if previous["days"] != 7 or previous["outcome"] != "success" or previous.get("fault"):
                continue
            if input_identity(previous["input"]) != input_identity(inputs[7]):
                raise ValueError("the seven-day clean reference used different CSV inputs")
            hashes = tuple(previous["output"][key] for key in OUTPUT_CHECKSUMS + SUBREGIONAL_CHECKSUMS)
            if references.setdefault(7, hashes) != hashes:
                raise ValueError("the seven-day clean reference contains different outputs")
        if 7 not in references:
            raise ValueError("collect verified seven-day clean trials before running recovery")
        for path in args.output_dir.glob("*.json"):
            previous = json.loads(path.read_bytes())
            if previous["fault"] != LOAD_FAULT or previous["outcome"] != "success":
                continue
            if (
                previous["days"] != 7 or input_identity(previous["input"]) != input_identity(inputs[7])
                or tuple(previous["output"][key] for key in OUTPUT_CHECKSUMS + SUBREGIONAL_CHECKSUMS) != references[7]
            ):
                raise ValueError("saved recovery trials use different inputs or outputs")
            completed_recovery[previous["runner"], previous["slots"]] += 1
    if args.mode != "clean":
        for first in range(len(DUAL_RUNNERS)):
            plan.append((args.output_dir, 7, DUAL_RUNNERS[first:] + DUAL_RUNNERS[:first], True))
    for directory, days, configurations, fault in plan:
        for runner, slots in configurations:
            if fault and completed_recovery[runner, slots]:
                completed_recovery[runner, slots] -= 1
                continue
            if runner == "airflow" and slots != active_slots:
                _ensure_slots(slots)
                active_slots = slots
            result = run_trial(args, airflow, days, runner, slots, inputs[days], authorities, directory, references, fault=fault)
            if result["outcome"] != "success":
                raise ValueError("trial failed verification; review its JSON record before continuing")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=("all", "clean", "recovery"), default="all",
                        help="run the full suite, clean trials, or recovery against existing clean trials")
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
        if not replay.csv_sources:
            raise ValueError("prepare a direct CSV benchmark manifest with experiments.import_eia930 --manifest-only")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        if args.mode != "recovery" and any(args.output_dir.iterdir()):
            raise ValueError("use an empty output directory for the authority batch suite")
        verify_runner_sources(replay, args.manifest, args.airflow_manifest)
        with requests.Session() as session:
            run_suite(args, Airflow(session), replay)
    except subprocess.CalledProcessError:
        parser.exit(1, "A Compose command failed. Check the Airflow stack and CSV mount.\n")
    except (KeyError, OSError, TypeError, ValueError, RuntimeError, psycopg.Error) as error:
        message = str(error) if isinstance(error, (OSError, RuntimeError, ValueError)) else type(error).__name__
        parser.exit(1, f"{message}\n")


if __name__ == "__main__":
    main()
