from datetime import date

import pytest

from renewflow.pipeline import observation_date
from renewflow.replay import read_manifest


def test_manual_airflow_run_requires_an_explicit_completed_utc_day():
    assert observation_date("2025-01-08") == date(2025, 1, 8)
    for invalid in (None, "20250108", "9999-01-01"):
        with pytest.raises(ValueError, match="completed UTC date"):
            observation_date(invalid)


def test_replay_manifest_cannot_escape_its_directory(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"snapshots": {"2025-01-15": "../outside.json"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="relative snapshot files"):
        read_manifest(manifest)
