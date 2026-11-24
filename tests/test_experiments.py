import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from experiments import benchmark
from experiments.airflow import Airflow, STAGES
from experiments.benchmark import attempt_metrics, prepare_input
from experiments.capture import snapshot_metadata
from experiments.report import summarize
from renewflow.db import connect_observations

MANIFEST = Path(__file__).parent / "fixtures" / "replay-manifest.json"


def test_suite_rejects_a_manifest_outside_the_airflow_mount_before_creating_trials(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.setenv("RENEWFLOW_REPLAY_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", [
        "benchmark", "--runner", "suite", "--start-date", "2025-01-15", "--end-date", "2025-01-16",
        "--manifest", str(MANIFEST), "--output-dir", str(tmp_path / "results"),
    ])
    prepare_database = Mock(side_effect=AssertionError("database setup started before checking the mount"))
    monkeypatch.setattr(benchmark, "_ensure_benchmark_database", prepare_database)

    with pytest.raises(SystemExit) as error:
        benchmark.main()

    assert error.value.code == 1
    assert f"RENEWFLOW_REPLAY_DIR={MANIFEST.parent}" in capsys.readouterr().err
    prepare_database.assert_not_called()


def test_native_backfill_dry_run_uses_inclusive_logical_bounds_across_year(monkeypatch):
    monkeypatch.delenv("RENEWFLOW_AIRFLOW_URL", raising=False)
    days = [date(2024, 12, 31), date(2025, 1, 1)]
    airflow = Airflow(Mock())
    request = Mock(return_value={"backfills": [
        {"logical_date": "2024-12-31T19:00:00Z"}, {"logical_date": "2025-01-01T19:00:00Z"},
    ]})
    monkeypatch.setattr(airflow, "call", request)

    body = airflow.prepare(days, 4, {"replay_manifest": "manifest.json"})

    assert body["from_date"] == "2024-12-31T19:00:00+00:00"
    assert body["to_date"] == "2025-01-01T19:00:00+00:00"
    assert body["max_active_runs"] == 4
    request.assert_called_once_with("POST", "/backfills/dry_run", json=body)
    request.return_value["backfills"].pop()
    with pytest.raises(RuntimeError, match="exactly the requested UTC dates"):
        airflow.prepare(days, 4, {})


def test_airflow_tracks_only_its_submitted_trial_and_reads_retry_history(monkeypatch):
    monkeypatch.delenv("RENEWFLOW_AIRFLOW_URL", raising=False)
    airflow = Airflow(Mock())
    start = "2025-01-15T19:00:00Z"
    end = "2025-01-15T19:00:06Z"
    run = {"logical_date": start, "dag_run_id": "backfill__test", "queued_at": start,
           "conf": {"benchmark_trial_id": "recorded"}}
    last_attempt = {"task_id": "extract_wind", "start_date": start, "end_date": end,
                    "try_number": 3, "state": "success", "duration": 6}
    request = Mock(side_effect=[
        {"id": 10}, {"completed_at": end},
        {"dag_runs": [run, {**run, "conf": {"benchmark_trial_id": "other"}}], "total_entries": 2},
        {"task_instances": [last_attempt]},
        {"task_instances": [
            {**last_attempt, "start_date": "2025-01-15T18:59:50Z", "try_number": 1},
            {**last_attempt, "state": "up_for_retry", "try_number": 2},
        ]},
    ])
    monkeypatch.setattr(airflow, "call", request)

    body = {"from_date": start, "to_date": start}
    assert airflow.submit(body, 10) == 10
    runs = airflow.runs(body, "recorded")
    assert runs == [run]
    attempts = airflow.attempts(runs)
    assert [(event["attempt"], event["status"], event["utc_date"]) for event in attempts] == [
        (1, "up_for_retry", "2025-01-15"), (2, "success", "2025-01-15"),
    ]
    assert [event["airflow_try_number"] for event in attempts] == [2, 3]


def test_replay_input_uses_real_fixture_and_rejects_gaps_and_changed_checksums(tmp_path):
    day = date(2025, 1, 15)
    fixture = MANIFEST.with_name(f"eia-ciso-{day}.json")
    metadata = snapshot_metadata(fixture, day)
    assert metadata["observations"] == metadata["source_rows"] == 72
    assert metadata["sha256"] == hashlib.sha256(fixture.read_bytes()).hexdigest()
    prepared = prepare_input(MANIFEST, [day])
    assert prepared["input_bytes"] == metadata["bytes"]
    assert len(prepared["expected_observation_sha256"]) == 64
    with pytest.raises(ValueError, match="no snapshot for 2025-01-16"):
        prepare_input(MANIFEST, [day, date(2025, 1, 16)])

    (tmp_path / fixture.name).write_bytes(fixture.read_bytes())
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"snapshots": {str(day): fixture.name}, "captures": {str(day): {"sha256": "wrong"}}}))
    with pytest.raises(ValueError, match="checksum differs"):
        prepare_input(manifest, [day])


def test_benchmark_schema_uses_separate_database_with_same_local_settings(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    monkeypatch.setenv("POSTGRES_DB", "renewflow")
    connect = Mock()
    monkeypatch.setattr("renewflow.db.psycopg.connect", connect)

    connect_observations()
    normal = connect.call_args.kwargs
    schema = "trial_" + "a" * 32
    connect_observations(benchmark_schema=schema)

    assert normal["dbname"] == "renewflow"
    assert connect.call_args.kwargs == normal | {
        "dbname": "renewflow_benchmark", "options": f"-c search_path={schema}",
    }
    with pytest.raises(ValueError, match="benchmark schema must be trial_"):
        connect_observations(benchmark_schema="public")


def test_benchmark_creates_its_database_on_first_use(monkeypatch):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.execute.return_value.fetchone.side_effect = [None, (1,)]
    monkeypatch.setattr(benchmark, "connect_observations", Mock(return_value=connection))

    benchmark._ensure_benchmark_database()
    benchmark._ensure_benchmark_database()

    lookup, create, second_lookup = connection.execute.call_args_list
    assert lookup.args[1] == ("renewflow_benchmark",)
    assert create.args[0].as_string() == 'CREATE DATABASE "renewflow_benchmark"'
    assert second_lookup == lookup


def test_recovery_metrics_count_only_retried_stage_and_time_until_daily_load():
    day = date(2025, 1, 15)
    events = [
        {"stage": stage, "utc_date": str(day), "attempt": 1, "status": "success",
         "finished_at": "2025-01-15T00:00:08+00:00"}
        for stage in STAGES
    ]
    wind = next(event for event in events if event["stage"] == "extract_wind")
    wind.update(status="up_for_retry", finished_at="2025-01-15T00:00:01+00:00")
    events.append({**wind, "attempt": 2, "status": "success", "finished_at": "2025-01-15T00:00:07+00:00"})
    metrics = attempt_metrics(events, [day])
    assert metrics["attempted_stages"] == 4
    assert metrics["first_attempt_successes"] == 3
    assert metrics["retries"] == metrics["recovery_repeated_stages"] == 1
    assert metrics["recovery_seconds"] == 7
    assert metrics["stages_without_first_attempt"] == []


def _trial(trial_id, seconds, *, fault=None):
    return {
        "trial_id": trial_id, "runner": "python", "slots": 1, "outcome": "success",
        "fault": fault, "start_date": "2025-01-15", "end_date": "2025-01-16", "days": 1,
        "wall_seconds": seconds, "observations_per_second": 72 / seconds,
        "input": {"captures": {"2025-01-15": {"sha256": "test-capture"}}, "input_bytes": 100},
        "output": {"observations": 72, "observation_sha256": "test-observations", "hourly_sha256": "test-hours",
                   "daily_sha256": "test-days", "days_published": 1},
    }


def test_suite_reuses_completed_trials_and_replaces_an_invalid_timing(monkeypatch, tmp_path):
    day = date(2025, 1, 15)
    source = tmp_path / "previous"
    destination = tmp_path / "continued"
    source.mkdir()
    destination.mkdir()
    inputs = prepare_input(MANIFEST, [day])
    successful = _trial("saved-python", 2) | benchmark._runtime_settings("18") | {"input": inputs}
    successful["output"].update(observation_sha256=inputs["expected_observation_sha256"], partial_days=0)
    successful["events"] = [
        {"utc_date": str(day), "stage": stage, "attempt": 1, "status": "success"} for stage in STAGES
    ]
    stalled = successful | {
        "trial_id": "stalled-airflow", "runner": "airflow", "airflow_version": benchmark.AIRFLOW_VERSION,
        "airflow_max_active_runs": 1, "poll_interval_seconds": 0.5,
        "airflow_runs": [{"queued_at": "2026-11-24T21:00:00Z", "end_date": "2026-11-24T21:05:00Z"}],
    }
    original = json.dumps(successful)
    (source / "saved-python.json").write_text(original)
    (source / "stalled-airflow.json").write_text(json.dumps(stalled))
    (source / "interrupted.json").write_text('{"trial_id":')

    connection = MagicMock()
    connection.__enter__.return_value.execute.return_value.fetchone.return_value = ("18",)
    monkeypatch.setattr(benchmark, "connect_observations", Mock(return_value=connection))
    monkeypatch.setattr(benchmark, "TIERS", (1,))
    monkeypatch.setattr(benchmark, "_ensure_slots", Mock())
    run_trial = Mock(return_value=successful)
    monkeypatch.setattr(benchmark, "trial", run_trial)
    args = SimpleNamespace(manifest=MANIFEST, resume_from=source, output_dir=destination)

    benchmark._suite(args, Mock(), [day])

    assert sum(call.args[3] == "python" for call in run_trial.call_args_list) == 4
    assert sum(call.args[3] == "airflow" for call in run_trial.call_args_list) == 10
    assert (source / "saved-python.json").read_text() == original
    assert json.loads((destination / "saved-python.json").read_text())["resumed_from"] == str(source / "saved-python.json")
    audit = json.loads((destination / "resume.json").read_text())
    assert audit["reused"] == ["saved-python.json"]
    assert audit["excluded"]["stalled-airflow.json"].startswith("Airflow recorded 300.00s")
    assert "interrupted.json" in audit["excluded"]


def test_report_uses_clean_trials_for_timing_and_checks_output_equivalence():
    trials = [_trial("a", 2), _trial("b", 4), _trial("c", 10, fault="2025-01-15:wind")]
    timing, reliability = summarize(trials)
    assert len(reliability) == 3
    assert timing[0]["trials"] == 2
    assert timing[0]["wall_seconds_median"] == 3
    trials[-1]["output"]["daily_sha256"] = "changed"
    with pytest.raises(ValueError, match="snapshots or SQL outputs differ"):
        summarize(trials)
