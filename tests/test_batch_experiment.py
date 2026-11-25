import json
from contextlib import nullcontext
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from experiments import batch_benchmark, capture_batch, report
from renewflow import batch, eia


def test_capture_groups_inclusive_eia_midnight_with_previous_utc_day(monkeypatch, tmp_path):
    start = batch.START_DATE
    tomorrow = start + timedelta(days=1)
    session = Mock()

    def fetch(_, metric, authority, start_date, end_date, *, api_key):
        assert (authority, start_date, end_date) == ("ERCO", start, start + timedelta(days=90))
        _, facet, code = eia.SERIES[metric]
        periods = eia._hour_endings(start) + eia._hour_endings(tomorrow)
        if metric == "demand":
            periods.pop()
        return eia.SeriesExtract([
            {
                "period": period, "respondent": authority, facet: code, "value": "10",
                "value-units": "megawatthours",
            }
            for period in periods
        ], "test-version")

    monkeypatch.setattr(capture_batch, "fetch_series_range", fetch)
    capture = capture_batch.capture_authority(session, "ERCO", tmp_path, "test-api-secret")
    assert set(capture["snapshots"]) == {str(start)}
    assert str(tomorrow) in capture["exclusions"]
    path = tmp_path / capture["snapshots"][str(start)]
    assert "test-api-secret" not in path.read_text()
    observations = batch.normalize_snapshot(path, start, "ERCO")
    assert len(observations) == 72
    assert set(observations["respondent"]) == {"ERCO"}
    assert observations["hour_start_utc"].max().isoformat() == "2025-01-15T23:00:00+00:00"
    with pytest.raises(ValueError, match="mismatched series identifiers"):
        batch.normalize_snapshot(path, start, "CISO")

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(capture))
    replay = batch.BatchReplay({"ERCO": manifest}, (1,))
    assert batch_benchmark.prepare_input(replay, 1)["expected_observations"] == 72

    connection = object()
    connect = Mock(return_value=nullcontext(connection))
    load = Mock(side_effect=[72, 0])
    monkeypatch.setattr(batch, "connect_observations", connect)
    monkeypatch.setattr(batch, "load_day", load)
    schema = "trial_" + "f" * 32
    assert batch.load_authority_batch(manifest, "ERCO", 1, schema=schema) == {"rows_validated": 72, "rows_changed": 72}
    assert batch.load_authority_batch(manifest, "ERCO", 1, schema=schema) == {"rows_validated": 72, "rows_changed": 0}
    assert all(call.kwargs == {"benchmark_schema": schema} for call in connect.call_args_list)
    assert all(call.args[0] is connection for call in load.call_args_list)

    path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError, match="snapshot checksum differs"):
        batch_benchmark.prepare_input(replay, 1)


def test_eia_range_requests_one_authority_and_exclusive_utc_end():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.return_value = {"response": {"total": 0, "data": []}}
    eia.fetch_series_range(
        session, "wind", "ERCO", batch.START_DATE, batch.START_DATE + timedelta(days=90),
        api_key="test-api-secret",
    )
    params = session.get.call_args.kwargs["params"]
    assert params["facets[respondent][]"] == "ERCO"
    assert params["facets[fueltype][]"] == "WND"
    assert params["start"] == "2025-01-15T01"
    assert params["end"] == "2025-04-15T00"


def test_select_cohorts_keeps_the_same_four_authorities_and_requires_ciso():
    def coverage(days):
        return {"snapshots": {str(batch.START_DATE + timedelta(days=offset)): "data.json" for offset in range(days)}}

    counts = {"CISO": 90, "ERCO": 30, "MISO": 90, "PJM": 90, "NYIS": 90, "ISNE": 90}
    captured = {authority: coverage(days) for authority, days in counts.items()}
    assert capture_batch.select_cohorts(captured) == (["CISO", "MISO", "PJM", "NYIS"], [7, 30, 90])
    del captured["NYIS"], captured["ISNE"]
    assert capture_batch.select_cohorts(captured) == (["CISO", "ERCO", "MISO", "PJM"], [7, 30])
    captured["NYIS"] = coverage(90)
    captured["CISO"] = coverage(29)
    assert capture_batch.select_cohorts(captured) == ([], [])


def test_suite_separates_pilots_and_rotates_three_repetitions(monkeypatch, tmp_path):
    replay = batch.BatchReplay({authority: Path(authority) for authority in batch.AUTHORITIES[:4]}, batch.COHORTS)
    args = SimpleNamespace(output_dir=tmp_path)
    airflow = Mock()
    airflow.call.return_value = {"is_paused": False}
    run = Mock(return_value={"outcome": "success"})
    monkeypatch.setattr(batch_benchmark, "prepare_input", lambda _, days: {"days": days})
    monkeypatch.setattr(batch_benchmark, "_ensure_benchmark_database", Mock())
    monkeypatch.setattr(batch_benchmark, "_ensure_slots", Mock())
    monkeypatch.setattr(batch_benchmark, "run_trial", run)

    batch_benchmark.run_suite(args, airflow, replay)
    configurations = batch_benchmark.DUAL_RUNNERS
    trials = [call.args for call in run.call_args_list]
    assert [(trial[2], trial[3:5]) for trial in trials[:3]] == [(30, config) for config in configurations]
    assert all(trial[7] == tmp_path / "pilot" for trial in trials[:3])
    assert all(trial[7] == tmp_path for trial in trials[3:])
    assert [(trial[2], trial[3:5]) for trial in trials[3:]] == [
        (days, config)
        for days in batch.COHORTS
        for first in range(3)
        for config in configurations[first:] + configurations[:first]
    ]


def test_airflow_trial_submits_one_run_and_reads_four_task_instances():
    args = SimpleNamespace(airflow_manifest="manifest.json", timeout=10)
    airflow = Mock()
    finished = {"state": "success"}
    tasks = [
        {"task_id": name, "state": "success", "try_number": 1, "start_date": None, "end_date": None, "duration": 0.5}
        for name in batch.BATCH_TASKS
    ]
    airflow.call.side_effect = [{}, finished, {"task_instances": tasks}]
    _, run, events = batch_benchmark._airflow_trial(args, airflow, "test", "trial-schema", 30)
    runs_path = f"/dags/{batch.BATCH_DAG}/dagRuns"
    assert airflow.call.call_args_list[0].args == ("POST", runs_path)
    assert airflow.call.call_args_list[0].kwargs["json"] == {
        "dag_run_id": "manual__test", "logical_date": None,
        "conf": {"replay_manifest": "manifest.json", "days": 30, "benchmark_schema": "trial-schema"},
    }
    assert airflow.call.call_args_list[-1].args == ("GET", f"{runs_path}/manual__test/taskInstances")
    assert run == finished
    assert [event["stage"] for event in events] == list(batch.BATCH_TASKS)


def test_trial_rejects_unequal_analytics_and_report_keeps_the_work_per_task(monkeypatch, tmp_path):
    args = SimpleNamespace(manifest=tmp_path / "manifest.json")
    inputs = {"captures": {}, "expected_observation_sha256": "obs", "input_bytes": 100}
    output = {
        "observations": 2016, "days_published": 28, "partial_days": 0, "observation_sha256": "obs",
        "hourly_sha256": "hourly", "daily_sha256": "daily",
    }
    events = [{"stage": stage, "status": "success", "attempt": 1} for stage in batch.BATCH_TASKS]
    monkeypatch.setattr(batch_benchmark, "_python_trial", Mock(return_value=(1.0, True, events)))
    monkeypatch.setattr(batch_benchmark, "_prepare_database", Mock(return_value="test"))
    monkeypatch.setattr(batch_benchmark, "_verify_database", Mock(return_value=output))
    references = {}
    authorities = batch.AUTHORITIES[:4]

    good = batch_benchmark.run_trial(args, Mock(), 7, "python", 1, inputs, authorities, tmp_path, references)
    output["hourly_sha256"] = "changed"
    bad = batch_benchmark.run_trial(args, Mock(), 7, "python", 1, inputs, authorities, tmp_path, references)
    assert bad["schema"] != good["schema"]
    assert bad["outcome"] == "failed"
    assert "analytics differ" in bad["error"]

    trials = [json.loads(path.read_text()) for path in tmp_path.glob("*.json")]
    timings, reliability = report.summarize(trials)
    assert len(reliability) == 2
    assert timings[0]["observations_per_task"] == 504
    assert timings[0]["trials"] == 1
