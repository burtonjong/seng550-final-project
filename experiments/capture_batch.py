"""Capture and select fixed EIA cohorts for the four-authority experiment."""

import argparse
import json
import os
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from experiments import utc_days
from experiments.capture import capture_metadata, fetch_with_retries
from renewflow.batch import AUTHORITIES, BATCH_TASKS, COHORTS, START_DATE, SUBREGION_CODE
from renewflow.eia import (
    SERIES, EIAError, SeriesExtract, _hour_endings, fetch_series_range, fetch_subregional_demand, save_snapshot,
)
from renewflow.transform import ValidationError, normalize_series, normalize_subregional_demand


def subregion_coverage(
    extract: SeriesExtract, authority: str, operating_days: dict[str, date], days: list[date]
) -> tuple[dict, dict]:
    """Index observed zones and find each zone's complete prefix from the start date."""
    rows_by_subregion = defaultdict(lambda: defaultdict(list))
    coverage = {}
    for row in extract.rows:
        code = row.get("subba")
        if row.get("parent") != authority or not isinstance(code, str) or not SUBREGION_CODE.fullmatch(code):
            raise ValidationError("subregional demand has a mismatched authority or invalid subregion code")
        period = row.get("period")
        day = operating_days.get(period) if isinstance(period, str) else None
        if day is None:
            coverage[code] = {"complete_prefix_days": 0, "first_exclusion": "invalid hour ending or timestamp outside the range"}
            continue
        rows_by_subregion[code][day].append(row)
    for code, by_day in rows_by_subregion.items():
        if code in coverage:
            continue
        complete = 0
        reason = None
        for day in days:
            try:
                normalize_subregional_demand(day, by_day.get(day, []), respondent=authority, subregions=(code,))
            except ValidationError as error:
                reason = str(error)
                break
            complete += 1
        coverage[code] = {"complete_prefix_days": complete, "first_exclusion": reason}
    return rows_by_subregion, coverage


def capture_authority(session: requests.Session, authority: str, directory: Path, api_key: str) -> dict:
    end = START_DATE + timedelta(days=COHORTS[-1])
    days = utc_days(START_DATE, end)
    operating_days = {period: day for day in days for period in _hour_endings(day)}
    rows_by_metric = {}
    versions = {}
    result = {
        "requested_end_date": end.isoformat(), "subregions": [], "subregion_coverage": {},
        "snapshots": {}, "captures": {}, "exclusions": {},
    }
    for metric in SERIES:
        try:
            extract = fetch_with_retries(
                fetch_series_range, session, metric, authority, START_DATE, end, api_key=api_key
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
    valid = {}
    for day in days:
        label = day.isoformat()
        extracts = {metric: SeriesExtract(rows_by_metric[metric][day], versions[metric]) for metric in SERIES}
        try:
            count = sum(
                len(normalize_series(metric, day, extract.rows, respondent=authority).observations)
                for metric, extract in extracts.items()
            )
        except ValidationError as error:
            result["exclusions"][label] = str(error)
            continue
        valid[day] = (extracts, count)
    base_days = next((index for index, day in enumerate(days) if day not in valid), len(days))
    if not base_days:
        return result
    try:
        subregional = fetch_with_retries(fetch_subregional_demand, session, authority, START_DATE, end, api_key=api_key)
        rows_by_subregion, coverage = subregion_coverage(subregional, authority, operating_days, days[:base_days])
    except ValidationError as error:
        result["exclusions"]["subregional_demand"] = str(error)
        return result
    except EIAError as error:
        raise EIAError(f"{authority} subregional demand: {error}") from None
    result["subregion_coverage"] = coverage
    available = max((zone["complete_prefix_days"] for zone in coverage.values()), default=0)
    if not available:
        result["exclusions"]["subregional_demand"] = "no subregion has a complete first UTC day"
        return result
    # Freeze the zone set for this authority across every smaller cohort.
    ceiling = max((size for size in COHORTS if size <= available), default=available)
    subregions = sorted(code for code, zone in coverage.items() if zone["complete_prefix_days"] >= ceiling)
    result["subregions"] = subregions
    for day in days[:ceiling]:
        label = day.isoformat()
        extracts, count = valid[day]
        zone_rows = [row for code in subregions for row in rows_by_subregion[code][day]]
        snapshot = directory / f"{day}.json"
        save_snapshot(
            snapshot, day, extracts, api_key=api_key, respondent=authority,
            subregional_demand=SeriesExtract(zone_rows, subregional.api_version),
        )
        result["snapshots"][label] = snapshot.name
        result["captures"][label] = capture_metadata(snapshot.read_bytes(), count + 24 * len(subregions))
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
    requested_end_date = (START_DATE + timedelta(days=COHORTS[-1])).isoformat()
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
                    if result.get("requested_end_date") != requested_end_date or "subregion_coverage" not in result:
                        raise ValueError("saved authority capture has a different range or dataset; choose a new output directory")
                else:
                    with TemporaryDirectory(dir=directory) as scratch:
                        staging = Path(scratch) / authority
                        staging.mkdir()
                        result = capture_authority(session, authority, staging, api_key)
                        (staging / "manifest.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
                        staging.rename(output)
                coverage[authority] = result
                print(f"{authority}: {len(result['snapshots'])} replay-ready UTC days, {len(result['subregions'])} subregions", flush=True)
                selected, cohorts = select_cohorts(coverage)
                if cohorts and cohorts[-1] == COHORTS[-1]:
                    break
        with selection.open("x", encoding="utf-8") as output:
            json.dump({
                "start_date": START_DATE.isoformat(),
                "requested_end_date": requested_end_date,
                "authorities": selected,
                "cohorts": cohorts,
                "authority_manifests": {authority: f"{authority}/manifest.json" for authority in selected},
                "subregions": {authority: coverage[authority]["subregions"] for authority in selected},
                "coverage": {
                    authority: {
                        "complete_days": len(result["snapshots"]), "exclusions": result["exclusions"],
                        "subregion_coverage": result["subregion_coverage"],
                    }
                    for authority, result in coverage.items()
                },
            }, output, indent=2)
            output.write("\n")
        print(f"Saved {selection}")
        if not cohorts:
            parser.exit(1, "CISO and three other authorities need 30 complete days with subregional demand before timing.\n")
        print(f"Selected {', '.join(selected)} for UTC-day cohorts {cohorts}")
    except (EIAError, KeyError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
