import json
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from renewflow import cli
from renewflow.eia import SERIES

SNAPSHOT = Path(__file__).parent / "fixtures" / "eia-ciso-2025-01-15.json"
DAY = date(2025, 1, 15)


@pytest.fixture
def loader(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only-password")
    monkeypatch.setattr(cli.psycopg, "connect", MagicMock())
    load = Mock(return_value=("committed", 72))
    monkeypatch.setattr(cli, "publish_day", load)
    return load


def test_snapshot_loads_one_complete_day(monkeypatch, loader, capsys):
    monkeypatch.setattr(sys, "argv", ["renewflow", "--snapshot", str(SNAPSHOT)])

    cli.main()

    loader.assert_called_once()
    observations = loader.call_args.args[1]
    assert observations.groupby("metric").size().to_dict() == dict.fromkeys(SERIES, 24)
    output = capsys.readouterr().out
    assert "validated 72 observations | inserted or updated 72" in output
    assert "negative generation hours 14" in output
    assert "Verified 1 UTC day(s)." in output


def test_snapshot_requires_all_three_series(tmp_path, monkeypatch, loader, capsys):
    path = tmp_path / "incomplete.json"
    path.write_text('{"utc_date": "2025-01-15", "frequency": "hourly", "series": {}}')
    monkeypatch.setattr(sys, "argv", ["renewflow", "--snapshot", str(path)])

    with pytest.raises(SystemExit) as error:
        cli.main()

    assert error.value.code == 1
    loader.assert_not_called()
    assert "snapshot with all three series" in capsys.readouterr().err


def test_replay_manifest_retries_only_the_failed_stage(tmp_path, monkeypatch, loader, capsys):
    capture = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    snapshots = {}
    # Shift the real fixture labels to exercise three dates from local files.
    for offset in (2, 1, 0):
        day = DAY + timedelta(days=offset)
        copy = json.loads(json.dumps(capture))
        copy["utc_date"] = day.isoformat()
        for source in copy["series"].values():
            for row in source["response"]["data"]:
                label, hour = row["period"].split("T")
                row["period"] = f"{date.fromisoformat(label) + timedelta(days=offset)}T{hour}"
        path = tmp_path / f"{day}.json"
        path.write_text(json.dumps(copy), encoding="utf-8")
        snapshots[day.isoformat()] = path.name
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"snapshots": snapshots}), encoding="utf-8")
    wait = Mock()
    monkeypatch.setattr(cli, "sleep", wait)
    monkeypatch.setattr(
        sys, "argv", ["renewflow", "--replay-manifest", str(manifest), "--fail-first", "2025-01-16:wind"]
    )

    cli.main()

    assert [call.args[1]["hour_start_utc"].iloc[0].date() for call in loader.call_args_list] == [
        DAY, DAY + timedelta(days=1), DAY + timedelta(days=2)
    ]
    wait.assert_called_once_with(5)
    events = [
        json.loads(line.removeprefix("renewflow_event "))
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("renewflow_event ")
    ]
    faults = [event for event in events if event["status"] == "failed"]
    assert [(event["utc_date"], event["stage"], event["attempt"], event["error_category"]) for event in faults] == [
        ("2025-01-16", "extract_wind", 1, "InjectedExtractFailure")
    ]
    assert len([event for event in events if event["stage"] == "extract_wind" and event["status"] == "success"]) == 3
    assert events[-1]["days_published"] == 3
