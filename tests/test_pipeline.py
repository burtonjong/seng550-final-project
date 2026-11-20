from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from renewflow.pipeline import observation_date
from renewflow.replay import read_manifest


@pytest.mark.parametrize("run_type", ["scheduled", "backfill"])
def test_airflow_observation_day_comes_from_interval_not_execution_time(run_type):
    interval_end = datetime(2025, 1, 16, 19, tzinfo=UTC)
    assert observation_date(run_type, interval_end, None) == date(2025, 1, 15)
    offset = timezone(timedelta(hours=8))
    assert observation_date(run_type, interval_end.astimezone(offset), None) == date(2025, 1, 15)


def test_manual_airflow_run_requires_an_explicit_utc_day():
    interval_end = datetime(2025, 1, 16, 19, tzinfo=UTC)
    assert observation_date("manual", interval_end, "2025-01-08") == date(2025, 1, 8)
    with pytest.raises(ValueError, match="manual runs require observation_date"):
        observation_date("manual", interval_end, None)
    with pytest.raises(ValueError, match="own UTC data interval"):
        observation_date("backfill", interval_end, "2025-01-08")


def test_replay_manifest_cannot_escape_its_directory(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"snapshots": {"2025-01-15": "../outside.json"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="relative snapshot files"):
        read_manifest(manifest)
