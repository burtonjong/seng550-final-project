import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from experiments import batch_benchmark, benchmark_support
from experiments.report import summarize
from renewflow.db import connect_observations

MANIFEST = Path(__file__).parent / "fixtures" / "eia-ciso-2025-01-15.json"


def test_suite_rejects_a_manifest_outside_the_airflow_mount_before_creating_trials(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.setenv("RENEWFLOW_REPLAY_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", [
        "batch_benchmark",
        "--manifest", str(MANIFEST), "--output-dir", str(tmp_path / "results"),
    ])
    prepare_database = Mock(side_effect=AssertionError("database setup started before checking the mount"))
    monkeypatch.setattr(batch_benchmark, "_ensure_benchmark_database", prepare_database)

    with pytest.raises(SystemExit) as error:
        batch_benchmark.main()

    assert error.value.code == 1
    assert f"RENEWFLOW_REPLAY_DIR={MANIFEST.parent}" in capsys.readouterr().err
    prepare_database.assert_not_called()


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
    monkeypatch.setattr(benchmark_support, "connect_observations", Mock(return_value=connection))

    benchmark_support._ensure_benchmark_database()
    benchmark_support._ensure_benchmark_database()

    lookup, create, second_lookup = connection.execute.call_args_list
    assert lookup.args[1] == ("renewflow_benchmark",)
    assert create.args[0].as_string() == 'CREATE DATABASE "renewflow_benchmark"'
    assert second_lookup == lookup


def _trial(trial_id, seconds, *, fault=None):
    return {
        "trial_id": trial_id, "runner": "python", "slots": 1, "outcome": "success",
        "fault": fault, "start_date": "2025-01-15", "end_date": "2025-01-16", "days": 1,
        "wall_seconds": seconds, "observations_per_second": 72 / seconds,
        "input": {"captures": {"2025-01-15": {"sha256": "test-capture"}}, "input_bytes": 100},
        "output": {"observations": 72, "observation_sha256": "test-observations", "hourly_sha256": "test-hours",
                   "daily_sha256": "test-days", "days_published": 1},
    }


def test_report_uses_clean_trials_for_timing_and_checks_output_equivalence():
    trials = [_trial("a", 2), _trial("b", 4), _trial("c", 10, fault="2025-01-15:wind")]
    timing, reliability = summarize(trials)
    assert len(reliability) == 3
    assert timing[0]["trials"] == 2
    assert timing[0]["wall_seconds_median"] == 3
    trials[-1]["output"]["daily_sha256"] = "changed"
    with pytest.raises(ValueError, match="inputs or SQL outputs differ"):
        summarize(trials)
