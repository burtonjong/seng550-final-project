import csv
import json
from datetime import date, datetime, time, timedelta

import pytest

from experiments import batch_benchmark, import_eia930 as source
from renewflow import eia930_csv as csv_source
from renewflow.batch import normalize_batch_snapshot, read_batch_replay
from renewflow.replay import read_manifest

FIRST = date(2025, 1, 15)
LATER = date(2025, 1, 17)


def write_days(directory, label, days, *, value="2", omit_wind=False):
    with (directory / f"{label}-balance.csv").open("w", newline="") as balance, (
        directory / f"{label}-zone.csv"
    ).open("w", newline="") as subregion:
        ba = csv.writer(balance)
        zones = csv.writer(subregion)
        ba.writerow([csv_source.AUTHORITY, csv_source.HOUR_END, *csv_source.COLUMNS.values(), *csv_source.BATTERY])
        zones.writerow([csv_source.AUTHORITY, csv_source.HOUR_END, csv_source.ZONE, csv_source.DEMAND])
        for day in days:
            for hour in range(1, 25):
                end = datetime.combine(day, time.min) + timedelta(hours=hour)
                period = end.strftime("%m/%d/%Y %I:%M:%S %p")
                for authority, codes in csv_source.ZONES.items():
                    wind = "" if omit_wind and hour == 24 and authority == "CISO" else value
                    ba.writerow([authority, period, "-1", wind, "4", "", "0"])
                    zones.writerows((authority, period, zone, "0") for zone in codes)


def test_benchmark_extracts_csvs_without_prebuilt_snapshots(tmp_path, monkeypatch):
    from renewflow import batch

    directory = tmp_path / "data"
    directory.mkdir()
    write_days(directory, "source", [FIRST, LATER])
    prepared = tmp_path / "benchmark"
    source.import_csv(directory, prepared, FIRST, manifest_only=True)
    assert [path.name for path in prepared.iterdir()] == ["manifest.json"]

    monkeypatch.setenv("RENEWFLOW_CSV_DIR", str(directory))
    monkeypatch.setenv("RENEWFLOW_STAGING_DIR", str(tmp_path / "staging"))
    replay = read_batch_replay(prepared / "manifest.json")
    csv_source.verify_sources(replay.csv_sources)
    expected = batch_benchmark.prepare_input(replay, 2)
    assert expected["expected_observations"] == 2304
    assert expected["input_bytes"] == 4 * expected["distinct_csv_bytes"]
    staged = []
    for runner, key in (("python", "a"), ("airflow", "b")):
        job = batch.authority_batch(replay, 2, 2, "trial_" + key * 32, False)
        raw = batch.execute_stage(job, "extract", None, runner=runner, attempt=1, emit=lambda _: None)
        normalized = batch.execute_stage(job, "transform", raw, runner=runner, attempt=1, emit=lambda _: None)
        extract, transform = batch.read_attempts(job.work.parent)
        assert extract["csv_bytes_scanned"] == expected["distinct_csv_bytes"]
        assert extract["rows_processed"] == transform["rows_processed"] == 432
        assert set(path.stem for path in (job.work / raw).glob("*.json")) == {str(FIRST), str(LATER)}
        staged.append([(job.work / normalized / f"{day}.json").read_bytes() for day in (FIRST, LATER)])
    assert staged[0] == staged[1]

    changed = directory / "source-balance.csv"
    changed.write_bytes(changed.read_bytes().replace(b",2,4,", b",3,4,", 1))
    with pytest.raises(ValueError, match="CSV checksum differs"):
        csv_source.verify_sources(replay.csv_sources)



def test_benchmark_manifest_reuses_only_matching_inputs(tmp_path, monkeypatch):
    inputs = tmp_path / "data"
    inputs.mkdir()
    write_days(inputs, "source", [FIRST, LATER])
    output = tmp_path / "benchmark"
    end = LATER + timedelta(days=1)
    source.import_csv(inputs, output, FIRST, end, manifest_only=True)
    manifest = output / "manifest.json"
    original = manifest.read_bytes()

    def fail_scan(*args, **kwargs):
        pytest.fail("matching benchmark manifest should be reused without parsing the CSVs")

    monkeypatch.setattr(source, "collect_rows", fail_scan)
    assert source.import_csv(inputs, output, FIRST, end, manifest_only=True) == json.loads(original)
    with pytest.raises(ValueError, match="matching benchmark manifest"):
        source.import_csv(inputs, output, FIRST, LATER, manifest_only=True)
    write_days(inputs, "source", [FIRST, LATER], value="3")
    with pytest.raises(ValueError, match="matching benchmark manifest"):
        source.import_csv(inputs, output, FIRST, end, manifest_only=True)
    assert manifest.read_bytes() == original


def test_extra_cohort_is_frozen_with_the_full_workload(tmp_path):
    inputs = tmp_path / "data"
    inputs.mkdir()
    write_days(inputs, "source", [FIRST, FIRST + timedelta(days=1), LATER])
    output = tmp_path / "benchmark"
    result = source.import_csv(inputs, output, FIRST, manifest_only=True, extra_cohorts=(2,))
    assert result["cohort_observations"] == {"1": 1152, "2": 2304, "3": 3456}
    replay = read_batch_replay(output / "manifest.json")
    assert replay.cohorts == (1, 2, 3)
    assert replay.selected_dates(2) == replay.selected_dates(3)[:2]
    with pytest.raises(ValueError, match="matching benchmark manifest"):
        source.import_csv(inputs, output, FIRST, manifest_only=True)
    with pytest.raises(ValueError, match="between 1 and 3"):
        source.import_csv(inputs, tmp_path / "too-long", FIRST, manifest_only=True, extra_cohorts=(4,))


def test_csv_extension_preserves_sources_and_uses_explicit_complete_dates(tmp_path):
    directory = tmp_path / "data"
    directory.mkdir()
    write_days(directory, "initial", [FIRST])
    original = tmp_path / "original"
    assert source.import_csv(directory, original, FIRST)["cohort_observations"] == {"1": 1152}
    frozen = (original / "manifest.json").read_bytes()

    write_days(directory, "extension", [FIRST, LATER], value="2.0")
    write_days(directory, "incomplete", [FIRST + timedelta(days=1)], omit_wind=True)
    extended = tmp_path / "extended"
    result = source.import_csv(directory, extended, FIRST)
    assert result["cohort_observations"] == {"1": 1152, "2": 2304}
    assert "missing, nonnumeric, or nonfinite" in result["coverage"]["CISO"]["exclusions"]["2025-01-16"]
    assert result["utc_dates"] == [str(FIRST), str(LATER)]
    assert len(result["source_files"]) == 6
    replay = read_batch_replay(extended / "manifest.json")
    assert replay.selected_dates(2) == (FIRST, LATER)
    inputs = batch_benchmark.prepare_input(replay, 2)
    assert inputs["expected_observations"] == 2304
    assert inputs["expected_subregional_observations"] == 1728
    path = read_manifest(replay.manifests["MISO"])[LATER]
    observations, subregional = normalize_batch_snapshot(path, LATER, "MISO", replay.subregions["MISO"])
    assert "0001" in set(subregional["subregion"])
    assert observations["hour_start_utc"].max().isoformat() == "2025-01-17T23:00:00+00:00"

    write_days(directory, "revision", [FIRST], value="3")
    refused = tmp_path / "conflict"
    with pytest.raises(ValueError, match="conflicting overlap"):
        source.import_csv(directory, refused, FIRST)
    assert not refused.exists()
    assert (original / "manifest.json").read_bytes() == frozen


def test_application_load_resumes_with_the_extended_manifest(tmp_path, monkeypatch, capsys):
    from renewflow import cli

    inputs = tmp_path / "data"
    inputs.mkdir()
    write_days(inputs, "initial", [FIRST])
    original = tmp_path / "original"
    source.import_csv(inputs, original, FIRST)
    finished = set()

    def publish(_, observations, *, subregional):
        authority = observations["respondent"].iat[0]
        key = authority, observations["hour_start_utc"].iat[0].date()
        assert set(subregional["subregion"]) == set(csv_source.ZONES[authority])
        if key in finished:
            return "skipped", 0
        finished.add(key)
        return "committed", len(observations) + len(subregional)

    monkeypatch.setattr(cli, "publish_day", publish)
    assert cli.load_manifest(None, original / "manifest.json", None, None) == 0
    assert len(finished) == 4
    capsys.readouterr()

    write_days(inputs, "new-period", [LATER])
    extended = tmp_path / "extended"
    source.import_csv(inputs, extended, FIRST)
    assert cli.load_manifest(None, extended / "manifest.json", None, None) == 0
    assert len(finished) == 8
    assert "committed 4 | updated 0 | skipped 4 | rejected 0" in capsys.readouterr().out
