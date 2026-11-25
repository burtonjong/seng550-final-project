"""Capture real CISO snapshots once and index their provenance for replay."""

import argparse
import hashlib
import json
import os
from datetime import date
from pathlib import Path
from time import sleep

import requests

from experiments import utc_days
from renewflow.eia import RESPONDENT, SERIES, EIAError, RetryableEIAError, fetch_series, save_snapshot
from renewflow.pipeline import EXTRACT_RETRIES, RETRY_DELAY_SECONDS
from renewflow.replay import read_snapshot
from renewflow.transform import ValidationError, normalize_series


def snapshot_metadata(path: Path, day: date, *, respondent: str = RESPONDENT) -> dict:
    captured_day, rows = read_snapshot(path)
    if captured_day != day:
        raise ValueError(f"{path.name} contains the wrong UTC date")
    records = sum(
        len(normalize_series(metric, day, rows[metric], respondent=respondent).observations) for metric in SERIES
    )
    return capture_metadata(path.read_bytes(), records)


def capture_metadata(raw: bytes, observations: int) -> dict:
    capture = json.loads(raw)
    sources = dict(capture["series"])
    if "subregional_demand" in capture:
        sources["subregional_demand"] = capture["subregional_demand"]
    return {
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "source_rows": sum(len(payload["response"]["data"]) for payload in sources.values()),
        "observations": observations,
        "captured_at_utc": capture.get("captured_at_utc"),
        "sources": {
            metric: {field: payload.get(field) for field in ("route", "facets", "apiVersion")}
            for metric, payload in sources.items()
        },
    }


def fetch_with_retries(fetch, *args, **kwargs):
    for attempt in range(EXTRACT_RETRIES + 1):
        try:
            return fetch(*args, **kwargs)
        except RetryableEIAError:
            if attempt == EXTRACT_RETRIES:
                raise
            sleep(RETRY_DELAY_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True, help="exclusive UTC end")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    try:
        days = utc_days(args.start_date, args.end_date)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        manifest = {"snapshots": {}, "captures": {}, "exclusions": {}}
        path = args.output_dir / "manifest.json"
        with requests.Session() as session:
            for day in days:
                label = day.isoformat()
                snapshot = args.output_dir / f"eia-ciso-{day}.json"
                try:
                    if not snapshot.exists():
                        if not api_key:
                            raise EIAError("set EIA_API_KEY to capture missing days")
                        extracts = {
                            metric: fetch_with_retries(fetch_series, session, metric, day, api_key=api_key)
                            for metric in SERIES
                        }
                        for metric, result in extracts.items():
                            normalize_series(metric, day, result.rows)
                        save_snapshot(snapshot, day, extracts, api_key=api_key)
                    manifest["captures"][label] = snapshot_metadata(snapshot, day)
                    manifest["snapshots"][label] = snapshot.name
                    print(f"{day} UTC ready for replay", flush=True)
                except ValidationError as error:
                    manifest["exclusions"][label] = str(error)
                    print(f"Excluded {day} UTC because its observations failed validation", flush=True)
                except EIAError as error:
                    manifest["capture_stopped"] = {"utc_date": label, "error_category": type(error).__name__}
                    raise EIAError(f"{day} UTC: {error}") from None
                finally:
                    temporary = path.with_suffix(".tmp")
                    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
                    temporary.replace(path)
        print(f"Saved replay manifest {path} with {len(manifest['snapshots'])} days")
        if manifest["exclusions"]:
            parser.exit(1, "Some requested dates were excluded. See manifest.json.\n")
    except (EIAError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
