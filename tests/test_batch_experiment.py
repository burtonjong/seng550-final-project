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
        assert (authority, start_date, end_date) == ("ERCO", start, start + timedelta(days=365))
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
    zone_rows = [
        {"period": period, "parent": "ERCO", "subba": code, "value": "0", "value-units": "megawatthours"}
        for code in ("COAS", "EAST")
        for period in eia._hour_endings(start)
    ]
    zone_rows.pop()
    monkeypatch.setattr(
        capture_batch, "fetch_subregional_demand", Mock(return_value=eia.SeriesExtract(zone_rows, "test-version")),
    )
    capture = capture_batch.capture_authority(session, "ERCO", tmp_path, "test-api-secret")
    assert set(capture["snapshots"]) == {str(start)}
    assert str(tomorrow) in capture["exclusions"]
    assert capture["subregions"] == ["COAS"]
    assert capture["subregion_coverage"]["EAST"]["complete_prefix_days"] == 0
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
    zones = ("COAS",)
    replay = batch.BatchReplay({"ERCO": manifest}, (1,), {"ERCO": zones})
    inputs = batch_benchmark.prepare_input(replay, 1)
    assert inputs["expected_observations"] == 96
    assert inputs["expected_subregional_observations"] == 24
    assert inputs["captures"][f"ERCO/{start}"]["sources"]["subregional_demand"]["facets"] == {"parent": "ERCO"}

    connection = object()
    connect = Mock(return_value=nullcontext(connection))
    load = Mock(side_effect=[96, 0])
    monkeypatch.setattr(batch, "connect_observations", connect)
    monkeypatch.setattr(batch, "load_day", load)
    schema = "trial_" + "f" * 32
    assert batch.load_authority_batch(manifest, "ERCO", 1, schema=schema, subregions=zones) == {
        "rows_validated": 96, "rows_changed": 96,
    }
    assert batch.load_authority_batch(manifest, "ERCO", 1, schema=schema, subregions=zones) == {
        "rows_validated": 96, "rows_changed": 0,
    }
    assert all(call.kwargs == {"benchmark_schema": schema} for call in connect.call_args_list)
    assert all(call.args[0] is connection for call in load.call_args_list)
    assert all(set(call.kwargs["subregional"]["subregion"]) == {"COAS"} for call in load.call_args_list)

    path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError, match="snapshot checksum differs"):
        batch_benchmark.prepare_input(replay, 1)


def test_eia_range_paginates_a_year_with_inclusive_midnight():
    session = Mock()
    rows = [
        {"period": period, "respondent": "ERCO", "fueltype": "WND"}
        for offset in range(365)
        for period in eia._hour_endings(batch.START_DATE + timedelta(days=offset))
    ]
    session.get.side_effect = [
        Mock(status_code=200, **{"json.return_value": {"response": {"total": len(rows), "data": page}}})
        for page in (rows[:5000], rows[5000:])
    ]
    extract = eia.fetch_series_range(
        session, "wind", "ERCO", batch.START_DATE, batch.START_DATE + timedelta(days=365),
        api_key="test-api-secret",
    )
    params = session.get.call_args.kwargs["params"]
    assert [call.kwargs["params"]["offset"] for call in session.get.call_args_list] == [0, 5000]
    assert params["facets[respondent][]"] == "ERCO"
    assert params["facets[fueltype][]"] == "WND"
    assert params["start"] == "2025-01-15T01"
    assert params["end"] == "2026-01-15T00"
    assert len(extract.rows) == 8760
    assert extract.rows[-1]["period"] == "2026-01-15T00"


def test_subregional_extraction_orders_pages_by_hour_and_zone():
    session = Mock()
    raw = [
        {"parent": "CISO", "subba": zone, "period": "2025-01-15T01", "value": "8", "ignored": "test-secret"}
        for zone in ("PGAE", "SCE")
    ]
    session.get.side_effect = [
        Mock(status_code=200, **{"json.return_value": {"response": {"total": 2, "data": [row]}}})
        for row in raw
    ]
    extract = eia.fetch_subregional_demand(
        session, "CISO", batch.START_DATE, batch.START_DATE + timedelta(days=1), api_key="test-secret",
    )
    assert [row["subba"] for row in extract.rows] == ["PGAE", "SCE"]
    assert all("ignored" not in row for row in extract.rows)
    assert [call.kwargs["params"]["offset"] for call in session.get.call_args_list] == [0, 1]
    url = session.get.call_args.args[0]
    params = session.get.call_args.kwargs["params"]
    assert url.endswith("/region-sub-ba-data/data/")
    assert params["facets[parent][]"] == "CISO"
    assert params["sort[1][column]"] == "subba"
    assert params["end"] == "2025-01-16T00"


def test_subregional_validation_separates_codes_and_rejects_conflicting_duplicates():
    from renewflow.transform import ValidationError, normalize_subregional_demand

    day = batch.START_DATE
    zones = ("PGAE", "SCE")
    rows = [
        {"parent": "CISO", "subba": zone, "period": period, "value": "0", "value-units": "megawatthours"}
        for zone in zones
        for period in eia._hour_endings(day)
    ]
    assert len(normalize_subregional_demand(day, rows + [rows[0]], respondent="CISO", subregions=zones)) == 48
    with pytest.raises(ValidationError, match="conflicting duplicate"):
        normalize_subregional_demand(day, rows + [rows[0] | {"value": "1"}], respondent="CISO", subregions=zones)
    with pytest.raises(ValidationError, match="missing hours for SCE"):
        normalize_subregional_demand(day, rows[:-1], respondent="CISO", subregions=zones)
    with pytest.raises(ValidationError, match="mismatched authority"):
        normalize_subregional_demand(day, rows, respondent="ERCO", subregions=zones)


def test_select_cohorts_keeps_the_same_four_authorities_and_requires_ciso():
    def coverage(days):
        return {"snapshots": {str(batch.START_DATE + timedelta(days=offset)): "data.json" for offset in range(days)}}

    counts = {"CISO": 365, "ERCO": 30, "MISO": 365, "PJM": 365, "NYIS": 365, "ISNE": 90}
    captured = {authority: coverage(days) for authority, days in counts.items()}
    assert capture_batch.select_cohorts(captured) == (["CISO", "MISO", "PJM", "NYIS"], [7, 30, 90, 365])
    captured["NYIS"] = coverage(364)
    assert capture_batch.select_cohorts(captured) == (["CISO", "MISO", "PJM", "NYIS"], [7, 30, 90])
    del captured["NYIS"], captured["ISNE"]
    assert capture_batch.select_cohorts(captured) == (["CISO", "ERCO", "MISO", "PJM"], [7, 30])
    captured["NYIS"] = coverage(90)
    captured["CISO"] = coverage(29)
    assert capture_batch.select_cohorts(captured) == ([], [])


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
    assert len(trials) == 39
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


@pytest.mark.parametrize("include_subregions", [False, True])
def test_trial_rejects_unequal_analytics_and_report_counts_all_rows(monkeypatch, tmp_path, include_subregions):
    args = SimpleNamespace(manifest=tmp_path / "manifest.json")
    authorities = batch.AUTHORITIES[:4]
    per_authority = 672 if include_subregions else 504
    inputs = {
        "captures": {}, "expected_observation_sha256": "obs", "input_bytes": 100,
        "observations_by_authority": dict.fromkeys(authorities, per_authority),
    }
    subregional = {
        "subregional_observations": 672, "partial_subregion_days": 0,
        "subregional_sha256": "zone", "daily_subregional_sha256": "daily-zone",
    }
    if include_subregions:
        inputs.update(expected_observations=2688, expected_subregional_observations=672, expected_subregional_sha256="zone")
    output = {
        "observations": 2016, "days_published": 28, "partial_days": 0, "observation_sha256": "obs",
        "hourly_sha256": "hourly", "daily_sha256": "daily",
    }
    events = [{"stage": stage, "status": "success", "attempt": 1} for stage in batch.BATCH_TASKS]
    monkeypatch.setattr(batch_benchmark, "_python_trial", Mock(return_value=(1.0, True, events)))
    monkeypatch.setattr(batch_benchmark, "_prepare_database", Mock(return_value="test"))
    monkeypatch.setattr(batch_benchmark, "_verify_database", lambda _: output.copy())
    monkeypatch.setattr(batch_benchmark, "_verify_subregional_output", lambda _: subregional.copy())
    references = {}

    good = batch_benchmark.run_trial(args, Mock(), 7, "python", 1, inputs, authorities, tmp_path, references)
    if include_subregions:
        subregional["daily_subregional_sha256"] = "changed"
    else:
        output["hourly_sha256"] = "changed"
    bad = batch_benchmark.run_trial(args, Mock(), 7, "python", 1, inputs, authorities, tmp_path, references)
    assert bad["schema"] != good["schema"]
    assert bad["outcome"] == "failed"
    assert "analytics differ" in bad["error"]

    trials = [json.loads(path.read_text()) for path in tmp_path.glob("*.json")]
    timings, reliability = report.summarize(trials)
    assert len(reliability) == 2
    if include_subregions:
        assert timings[0]["observations"] == 2688
        assert timings[0]["subregional_observations"] == 672
        assert good["events"][0]["expected_observations"] == 672
    else:
        assert timings[0]["observations_per_task"] == 504
    assert timings[0]["trials"] == 1
