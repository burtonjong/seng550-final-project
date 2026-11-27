"""Build immutable replay manifests from local EIA-930 CSV files."""

import argparse
import csv
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from experiments.snapshot_metadata import snapshot_file_metadata
from renewflow.batch import START_DATE, normalize_batch_rows, read_batch_replay, replay_cohorts
from renewflow.eia import SERIES
from renewflow.eia930_csv import ZONES, collect_rows, day_payload, describe_source
from renewflow.replay import snapshot_rows
from renewflow.transform import ValidationError

ONE_DAY = timedelta(days=1)


def import_csv(
    input_dir: Path, output: Path, start: date, end: date | None = None, *,
    manifest_only: bool = False, extra_cohorts: tuple[int, ...] = (),
) -> dict:
    if extra_cohorts and not manifest_only:
        raise ValueError("extra cohorts require --manifest-only")
    extra_cohorts = tuple(sorted(set(extra_cohorts)))
    today = datetime.now(UTC).date()
    if end is not None and end > today:
        raise ValueError("choose completed UTC days")
    if start >= (end or today):
        raise ValueError("end date must be later than start date")
    if output.exists() and not manifest_only:
        raise ValueError("replay directory already exists; choose a new output directory")
    paths = tuple(sorted(input_dir.glob("*.csv")))
    sources = [describe_source(path) for path in paths]
    records = None if end is not None else collect_rows(paths, start, today)
    stop = end if end is not None else max(day for _, day in records) + ONE_DAY
    if output.exists():
        manifest = output / "manifest.json"
        if manifest.is_file():
            saved = json.loads(manifest.read_text(encoding="utf-8"))
            if (
                isinstance(saved, dict) and saved.get("version") == 2 and saved.get("source_files") == sources
                and saved.get("requested_start_date") == str(start) and saved.get("requested_end_date") == str(stop)
                and saved.get("subregions") == {authority: list(zones) for authority, zones in ZONES.items()}
                and saved.get("extra_cohorts", []) == list(extra_cohorts)
                and isinstance(saved.get("cohort_observations"), dict)
            ):
                read_batch_replay(manifest)
                return saved
        raise ValueError("existing directory does not contain a matching benchmark manifest; choose a new output directory")
    if records is None:
        records = collect_rows(paths, start, stop)
    dates = [start + timedelta(days=index) for index in range((stop - start).days)]
    coverage = {}
    manifests = {}
    shared = set(dates)
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output.parent) as scratch:
        directory = Path(scratch) / "replay"
        directory.mkdir()
        for authority in ZONES:
            target = directory / authority
            if not manifest_only:
                target.mkdir()
            entries, captures, exclusions = {}, {}, {}
            for day in dates:
                label = day.isoformat()
                values = records.get((authority, day))
                if not values:
                    exclusions[label] = "no selected observations"
                    continue
                try:
                    payload = day_payload(day, authority, values)
                    rows = snapshot_rows(payload, include_subregions=True)
                    normalize_batch_rows(rows, day, authority, ZONES[authority])
                except ValidationError as error:
                    exclusions[label] = str(error)
                    continue
                filename = f"{day}.json"
                entries[label] = filename
                if not manifest_only:
                    raw = json.dumps(payload, separators=(",", ":")).encode()
                    (target / filename).write_bytes(raw)
                    captures[label] = snapshot_file_metadata(raw, sum(map(len, rows.values())))
            if not manifest_only:
                manifest = target / "manifest.json"
                manifest.write_text(json.dumps({"snapshots": entries, "captures": captures}, indent=2) + "\n")
                manifests[authority] = f"{authority}/manifest.json"
            coverage[authority] = {"complete_days": len(entries), "exclusions": exclusions}
            shared.intersection_update(date.fromisoformat(day) for day in entries)
        if not shared:
            raise ValueError("the four authorities have no shared complete UTC days")
        selected = sorted(shared)
        cohorts = list(replay_cohorts(len(selected), extra_cohorts))
        result = {
            "version": 2 if manifest_only else 1, "source_files": sources, "authorities": list(ZONES),
            "requested_start_date": str(start), "requested_end_date": str(stop),
            "start_date": str(selected[0]), "utc_dates": [str(day) for day in selected],
            "cohorts": cohorts, "subregions": ZONES,
            **({} if manifest_only else {"authority_manifests": manifests}),
            **({"extra_cohorts": extra_cohorts} if extra_cohorts else {}),
            "excluded_utc_dates": [str(day) for day in dates if day not in shared],
            "coverage": coverage,
            "cohort_observations": {
                str(size): size * 24 * sum(len(SERIES) + len(zones) for zones in ZONES.values())
                for size in cohorts
            },
        }
        (directory / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
        directory.rename(output)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, default=START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, help="exclusive UTC end")
    parser.add_argument("--manifest-only", action="store_true", help="prepare direct CSV benchmarks without snapshots")
    parser.add_argument(
        "--extra-cohort", type=int, action="append", default=[],
        help="add a benchmark workload of N complete UTC days (repeatable)",
    )
    args = parser.parse_args()
    try:
        result = import_csv(
            args.input_dir, args.output_dir, args.start_date, args.end_date,
            manifest_only=args.manifest_only, extra_cohorts=tuple(args.extra_cohort),
        )
    except (KeyError, OSError, TypeError, ValueError, csv.Error) as error:
        parser.exit(1, f"{error}\n")
    print(f"Manifest ready at {args.output_dir / 'manifest.json'}")
    for size, count in result["cohort_observations"].items():
        print(f"{size} UTC days | {count:,} observations")


if __name__ == "__main__":
    main()
