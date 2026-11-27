import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from experiments import batch_benchmark, report
from renewflow import batch


def test_subregional_validation_separates_codes_and_rejects_conflicting_duplicates():
    from renewflow.transform import ValidationError, normalize_subregional_demand

    day = batch.START_DATE
    zones = ("PGAE", "SCE")
    rows = [
        {"parent": "CISO", "subba": zone, "period": period, "value": "0", "value-units": "megawatthours"}
        for zone in zones
        for period in [(datetime.combine(day, time.min) + timedelta(hours=hour)).isoformat(timespec="hours")
                       for hour in range(1, 25)]
    ]
    assert len(normalize_subregional_demand(day, rows + [rows[0]], respondent="CISO", subregions=zones)) == 48
    with pytest.raises(ValidationError, match="conflicting duplicate"):
        normalize_subregional_demand(day, rows + [rows[0] | {"value": "1"}], respondent="CISO", subregions=zones)
    with pytest.raises(ValidationError, match="missing hours for SCE"):
        normalize_subregional_demand(day, rows[:-1], respondent="CISO", subregions=zones)
    with pytest.raises(ValidationError, match="mismatched authority"):
        normalize_subregional_demand(day, rows, respondent="ERCO", subregions=zones)


@pytest.mark.parametrize("cohorts", [(7, 30), (7, 30, 90), (7, 30, 90, 365), (7, 30, 365)])
def test_batch_manifest_accepts_only_contiguous_cohorts(tmp_path, cohorts):
    manifest = tmp_path / "manifest.json"
    authorities = batch.AUTHORITIES[:4]
    manifest.write_text(json.dumps({
        "start_date": "2025-01-15",
        "authorities": authorities,
        "cohorts": cohorts,
        "authority_manifests": {authority: f"{authority}/manifest.json" for authority in authorities},
    }))
    if cohorts == (7, 30, 365):
        with pytest.raises(ValueError, match="invalid authority selection or UTC cohorts"):
            batch.read_batch_replay(manifest)
    else:
        assert batch.read_batch_replay(manifest).cohorts == cohorts


def test_suite_separates_pilots_and_rotates_three_repetitions(monkeypatch, tmp_path):
    replay = batch.BatchReplay({authority: Path(authority) for authority in batch.AUTHORITIES[:4]}, (*batch.COHORTS, 334))
    args = SimpleNamespace(output_dir=tmp_path, mode="all")
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
    assert len(trials) == 48
    assert [(trial[2], trial[3:5]) for trial in trials[:3]] == [(30, config) for config in configurations]
    assert all(trial[7] == tmp_path / "pilot" for trial in trials[:3])
    assert all(trial[7] == tmp_path for trial in trials[3:])
    assert [(trial[2], trial[3:5]) for trial in trials[3:39]] == [
        (days, config)
        for days in replay.cohorts
        for first in range(3)
        for config in configurations[first:] + configurations[:first]
    ]
    assert all(call.kwargs["fault"] for call in run.call_args_list[-9:])
    assert all(trial[2] == 7 for trial in trials[-9:])


def test_airflow_trial_submits_one_run_and_reads_twelve_task_instances():
    args = SimpleNamespace(airflow_manifest="manifest.json", timeout=10)
    airflow = Mock()
    finished = {"state": "success"}
    tasks = [
        {"task_id": name, "state": "success", "try_number": 1, "start_date": None, "end_date": None, "duration": 0.5}
        for name in batch.BATCH_TASKS
    ]
    airflow.call.side_effect = [{}, finished, {"task_instances": tasks}]
    _, run, instances = batch_benchmark._airflow_trial(args, airflow, "test", "trial-schema", 30, False)
    runs_path = f"/dags/{batch.BATCH_DAG}/dagRuns"
    assert airflow.call.call_args_list[0].args == ("POST", runs_path)
    assert airflow.call.call_args_list[0].kwargs["json"] == {
        "dag_run_id": "manual__test", "logical_date": None,
        "conf": {
            "replay_manifest": "manifest.json", "days": 30, "benchmark_schema": "trial-schema",
            "inject_load_failure": False,
        },
    }
    assert airflow.call.call_args_list[-1].args == ("GET", f"{runs_path}/manual__test/taskInstances")
    assert run == finished
    assert instances == tasks
    assert len(instances) == 12


def test_trial_rejects_unequal_analytics_and_report_counts_all_rows(monkeypatch, tmp_path):
    args = SimpleNamespace(manifest=tmp_path / "manifest.json")
    authorities = batch.AUTHORITIES[:4]
    inputs = {
        "captures": {}, "expected_observation_sha256": "obs", "input_bytes": 100,
        "observations_by_authority": dict.fromkeys(authorities, 672),
        "expected_observations": 2688, "expected_subregional_observations": 672, "expected_subregional_sha256": "zone",
    }
    subregional = {
        "subregional_observations": 672, "partial_subregion_days": 0,
        "subregional_sha256": "zone", "daily_subregional_sha256": "daily-zone",
    }
    output = {
        "observations": 2016, "days_published": 28, "partial_days": 0, "observation_sha256": "obs",
        "hourly_sha256": "hourly", "daily_sha256": "daily",
    }
    start = datetime.now(UTC)
    events = [
        {
            "stage": stage, "status": "success", "attempt": 1, "authority": authorities[index // 3],
            "started_at": (start + timedelta(milliseconds=index * 2)).isoformat(),
            "finished_at": (start + timedelta(milliseconds=index * 2 + 1)).isoformat(),
            "rows_processed": 672, "days_processed": 7,
        }
        for index, stage in enumerate(batch.BATCH_TASKS)
    ]
    monkeypatch.setattr(batch, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(batch_benchmark, "read_attempts", lambda _: events)
    monkeypatch.setattr(batch_benchmark, "_python_trial", Mock(return_value=(1.0, True)))
    monkeypatch.setattr(batch_benchmark, "_prepare_database", Mock(return_value="test"))
    monkeypatch.setattr(batch_benchmark, "_verify_database", lambda _: output.copy())
    monkeypatch.setattr(batch_benchmark, "_verify_subregional_output", lambda _: subregional.copy())
    references = {}

    good = batch_benchmark.run_trial(args, Mock(), 7, "python", 1, inputs, authorities, tmp_path, references)
    subregional["daily_subregional_sha256"] = "changed"
    bad = batch_benchmark.run_trial(args, Mock(), 7, "python", 1, inputs, authorities, tmp_path, references)
    assert bad["schema"] != good["schema"]
    assert bad["outcome"] == "failed"
    assert "analytics differ" in bad["error"]

    trials = [json.loads(path.read_text()) for path in tmp_path.glob("*.json")]
    timings, reliability = report.summarize(trials)
    assert len(reliability) == 2
    assert timings[0]["observations"] == 2688
    assert timings[0]["subregional_observations"] == 672
    assert good["events"][0]["expected_observations"] == 672
    assert timings[0]["trials"] == 1
