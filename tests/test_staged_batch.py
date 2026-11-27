import json
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from runpy import run_path
from unittest.mock import Mock

import pytest

from experiments import batch_benchmark, import_eia930, report
from renewflow import batch


def test_load_retry_reuses_transformed_days_and_publishes_each_row_once(tmp_path, monkeypatch, write_days):
    source = tmp_path / "source"
    source.mkdir()
    dates = tuple(batch.START_DATE + timedelta(days=offset) for offset in (0, 1, 2, 4, 5, 6, 7))
    write_days(source, "test", dates)
    prepared = tmp_path / "benchmark"
    import_eia930.import_csv(source, prepared, dates[0], manifest_only=True)
    replay = batch.read_batch_replay(prepared / "manifest.json")
    monkeypatch.setenv("RENEWFLOW_CSV_DIR", str(source))
    monkeypatch.setattr(batch, "STAGING_ROOT", tmp_path / "stage")
    context = batch.authority_batch(replay, 0, 7, "trial_" + "1" * 32, True)
    stored = {}

    def load(_, observations, *, subregional):
        before = len(stored)
        for authority, hour, metric, value in observations.itertuples(index=False, name=None):
            stored[authority, hour, metric] = value
        for authority, hour, zone, value in subregional.itertuples(index=False, name=None):
            stored[authority, hour, zone] = value
        return len(stored) - before

    monkeypatch.setattr(batch, "connect_observations", lambda **kwargs: nullcontext(object()))
    monkeypatch.setattr(batch, "load_day", load)
    emit = Mock()
    raw = batch.execute_stage(context, "extract", None, runner="python", attempt=1, emit=emit)
    normalized = batch.execute_stage(context, "transform", raw, runner="python", attempt=1, emit=emit)
    with pytest.raises(batch.InjectedLoadFailure):
        batch.execute_stage(context, "load", normalized, runner="python", attempt=1, emit=emit)
    assert len(stored) == 504
    for snapshot in source.glob("*.csv"):
        snapshot.unlink()
    batch.execute_stage(context, "load", normalized, runner="python", attempt=2, emit=emit)
    assert len(stored) == 1176
    assert {hour.date() for _, hour, _ in stored} == set(dates)
    events = batch.read_attempts(context.work.parent)
    assert [(event["stage"], event["attempt"], event["status"]) for event in events] == [
        ("extract_authority_0", 1, "success"), ("transform_authority_0", 1, "success"),
        ("load_authority_0", 1, "failed"), ("load_authority_0", 2, "success"),
    ]
    assert events[-2]["days_committed"] == 3
    assert events[-1]["days_committed"] == 7
    assert events[-1]["rows_changed"] == 672
    attempts = events + [
        {
            **events[0], "stage": name, "authority": batch.AUTHORITIES[index // 3], "status": "success",
            "days_processed": 7, "rows_processed": 1176,
        }
        for index, name in enumerate(batch.BATCH_TASKS) if index >= 3
    ]
    recovery = batch_benchmark._verify_attempts(
        attempts, True, 7, {"observations_by_authority": dict.fromkeys(batch.AUTHORITIES, 1176)},
    )
    assert recovery["recovery_repeated_days"] == 3
    assert recovery["recovery_repeated_rows"] == 504
    assert recovery["recovery_required_operator"] is False

    staged = context.work / "raw" / f"{batch.START_DATE}.json"
    broken = json.loads(staged.read_bytes())
    broken["series"]["solar"]["response"]["data"].pop()
    staged.write_text(json.dumps(broken))
    with pytest.raises(ValueError, match="missing hours"):
        batch.execute_stage(context, "transform", raw, runner="python", attempt=2, emit=emit)


def test_airflow_has_four_independent_three_stage_chains():
    pytest.importorskip("airflow.sdk")
    path = Path(__file__).resolve().parents[1] / "dags" / "renewflow_authority_batches.py"
    dag = run_path(str(path))["dag_instance"]
    assert len(dag.tasks) == 12
    for index in range(4):
        extract = dag.get_task(f"extract_authority_{index}")
        transform = dag.get_task(f"transform_authority_{index}")
        load = dag.get_task(f"load_authority_{index}")
        assert extract.upstream_task_ids == set()
        assert transform.upstream_task_ids == {extract.task_id}
        assert load.upstream_task_ids == {transform.task_id}
        assert load.downstream_task_ids == set()
        assert extract.retries == transform.retries == 0
        assert load.retries == 1
        assert load.retry_delay.total_seconds() == 5


def test_measured_overlap_and_speedup_are_reported_separately():
    launch = datetime(2025, 1, 15, tzinfo=UTC)
    events = [
        {
            "stage": f"extract_authority_{index}", "attempt": 1, "status": "success",
            "started_at": (launch + timedelta(seconds=start)).isoformat(),
            "finished_at": (launch + timedelta(seconds=end)).isoformat(),
        }
        for index, (start, end) in enumerate(((2, 6), (3, 5), (4, 6), (6, 7)))
    ]
    metrics = batch_benchmark._attempt_metrics(events, launch)
    assert metrics["first_stage_delay_seconds"] == 2
    assert metrics["peak_concurrent_stages"] == 3
    assert metrics["stage_overlap_seconds"] == 3
    rows = [
        {
            "runner": runner, "slots": slots, "wall_seconds_median": seconds, "days": 7,
            "start_date": "2025-01-15", "end_date": "2025-01-22", "observations": 8400,
        }
        for runner, slots, seconds in (("python", 1, 8), ("airflow", 1, 24), ("airflow", 4, 12))
    ]
    comparison = report.comparisons(rows)[0]
    assert comparison["airflow_four_vs_one_speedup"] == 2
    assert comparison["airflow_four_vs_python_speedup"] == pytest.approx(2 / 3)
    assert not comparison["airflow_four_faster_than_python"]
