"""Capture and select fixed EIA cohorts for the four-authority experiment."""

import argparse
import json
import os
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from experiments import utc_days
from experiments.capture import fetch_with_retries, snapshot_metadata
from renewflow.batch import AUTHORITIES, BATCH_TASKS, COHORTS, START_DATE
from renewflow.eia import SERIES, EIAError, SeriesExtract, _hour_endings, fetch_series_range, save_snapshot
from renewflow.transform import ValidationError, normalize_series


def capture_authority(session: requests.Session, authority: str, directory: Path, api_key: str) -> dict:
    days = utc_days(START_DATE, START_DATE + timedelta(days=COHORTS[-1]))
    operating_days = {period: day for day in days for period in _hour_endings(day)}
    rows_by_metric = {}
    versions = {}
    result = {"snapshots": {}, "captures": {}, "exclusions": {}}
    for metric in SERIES:
        try:
            extract = fetch_with_retries(
                fetch_series_range, session, metric, authority, days[0], days[-1] + timedelta(days=1), api_key=api_key
            )
        except EIAError as error:
            raise EIAError(f"{authority} {metric}: {error}") from None
        rows_by_day = defaultdict(list)
        for row in extract.rows:
            period = row.get("period")
            day = operating_days.get(period) if isinstance(period, str) else None
            if day is None:
                result["exclusions"]["range"] = f"{metric}: invalid hour ending or timestamp outside the range"
                return result
            rows_by_day[day].append(row)
        rows_by_metric[metric] = rows_by_day
        versions[metric] = extract.api_version
    for day in days:
        label = day.isoformat()
        extracts = {metric: SeriesExtract(rows_by_metric[metric][day], versions[metric]) for metric in SERIES}
        try:
            for metric, extract in extracts.items():
                normalize_series(metric, day, extract.rows, respondent=authority)
        except ValidationError as error:
            result["exclusions"][label] = str(error)
            continue
        snapshot = directory / f"{day}.json"
        save_snapshot(snapshot, day, extracts, api_key=api_key, respondent=authority)
        result["snapshots"][label] = snapshot.name
        result["captures"][label] = snapshot_metadata(snapshot, day, respondent=authority)
    return result


def select_cohorts(coverage: dict[str, dict]) -> tuple[list[str], list[int]]:
    for ceiling in reversed(COHORTS[1:]):
        required = {
            (START_DATE + timedelta(days=offset)).isoformat() for offset in range(ceiling)
        }
        eligible = [
            authority for authority in AUTHORITIES if required <= coverage.get(authority, {}).get("snapshots", {}).keys()
        ]
        if len(eligible) >= len(BATCH_TASKS) and eligible[0] == "CISO":
            return eligible[:len(BATCH_TASKS)], [days for days in COHORTS if days <= ceiling]
    return [], []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not api_key:
        parser.error("set EIA_API_KEY in the runtime environment")
    directory = args.output_dir
    selection = directory / "manifest.json"
    try:
        if selection.exists():
            raise ValueError("the capture already has a selection manifest; choose a new output directory")
        directory.mkdir(parents=True, exist_ok=True)
        coverage = {}
        with requests.Session() as session:
            for authority in AUTHORITIES:
                output = directory / authority
                if output.is_dir():
                    result = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
                else:
                    with TemporaryDirectory(dir=directory) as scratch:
                        staging = Path(scratch) / authority
                        staging.mkdir()
                        result = capture_authority(session, authority, staging, api_key)
                        (staging / "manifest.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
                        staging.rename(output)
                coverage[authority] = result
                print(f"{authority}: {len(result['snapshots'])} complete UTC days", flush=True)
                selected, cohorts = select_cohorts(coverage)
                if cohorts and cohorts[-1] == COHORTS[-1]:
                    break
        with selection.open("x", encoding="utf-8") as output:
            json.dump({
                "start_date": START_DATE.isoformat(),
                "authorities": selected,
                "cohorts": cohorts,
                "authority_manifests": {authority: f"{authority}/manifest.json" for authority in selected},
                "coverage": {
                    authority: {"complete_days": len(result["snapshots"]), "exclusions": result["exclusions"]}
                    for authority, result in coverage.items()
                },
            }, output, indent=2)
            output.write("\n")
        print(f"Saved {selection}")
        if not cohorts:
            parser.exit(1, "CISO and three other authorities need 30 complete days before timing.\n")
        print(f"Selected {', '.join(selected)} for UTC-day cohorts {cohorts}")
    except (EIAError, KeyError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
