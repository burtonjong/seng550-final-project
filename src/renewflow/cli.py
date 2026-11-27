"""Load local EIA-930 replay data into PostgreSQL using sequential Python."""

import argparse
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import sleep
from uuid import uuid4

import pandas as pd
import psycopg

from renewflow.batch import normalize_batch_snapshot, read_batch_replay
from renewflow.db import connect_observations, publish_day
from renewflow.eia import SERIES
from renewflow.pipeline import (
    EXTRACT_RETRIES,
    InjectedExtractFailure,
    RETRY_DELAY_SECONDS,
    fail_on_first_attempt,
    record_attempt,
    replay_fault,
)
from renewflow.replay import read_manifest, read_series, read_snapshot
from renewflow.transform import normalize_series


def _extract(day, metric, snapshot, fault, run_id):
    for attempt in range(1, EXTRACT_RETRIES + 2):
        try:
            with record_attempt(
                print, runner="python", run_id=run_id, stage=f"extract_{metric}",
                day=day, attempt=attempt,
            ) as counts:
                fail_on_first_attempt(fault, day, metric, attempt)
                rows = read_series(snapshot, day, metric)
                counts["rows_received"] = len(rows)
                result = normalize_series(metric, day, rows)
                counts["rows_validated"] = len(result.observations)
                counts["identical_duplicates"] = result.identical_duplicates
                counts["negative_generation_hours"] = result.negative_generation_hours
                return result
        except InjectedExtractFailure:
            if attempt > EXTRACT_RETRIES:
                raise
            sleep(RETRY_DELAY_SECONDS)


def load_manifest(connection, manifest: Path, start: date | None, end: date | None) -> int:
    replay = read_batch_replay(manifest)
    if not replay.dates or replay.csv_sources:
        raise ValueError("--manifest requires an application replay manifest with snapshots")
    totals = dict.fromkeys(("committed", "updated", "skipped", "rejected"), 0)
    for authority, path in replay.manifests.items():
        snapshots = read_manifest(path)
        missing = set(replay.dates).difference(snapshots)
        if missing:
            raise ValueError(f"{authority} has no snapshot entry for {min(missing)}")
        for day, snapshot in sorted(snapshots.items()):
            if start is not None and not start <= day < end:
                continue
            try:
                rows, zones = normalize_batch_snapshot(snapshot, day, authority, replay.subregions[authority])
            except (OSError, ValueError) as error:
                totals["rejected"] += 1
                print(f"{authority} {day} UTC | rejected | {error}", flush=True)
                continue
            status, changed = publish_day(connection, rows, subregional=zones)
            totals[status] += 1
            print(f"{authority} {day} UTC | {status} | inserted or updated {changed}", flush=True)
    print("PostgreSQL load summary (authority-days) | " + " | ".join(
        f"{status} {count}" for status, count in totals.items()
    ))
    if not sum(totals.values()):
        raise ValueError("no replay days match the requested UTC range")
    return totals["rejected"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest", type=Path, help="load a multi-authority CSV import manifest")
    source.add_argument("--snapshot", type=Path, help="load one day from a sanitized EIA snapshot")
    source.add_argument("--replay-manifest", type=Path, help="replay snapshots listed by UTC date")
    parser.add_argument("--start-date", type=date.fromisoformat, help="first UTC date to load")
    parser.add_argument("--end-date", type=date.fromisoformat, help="exclusive end of the UTC range")
    parser.add_argument("--run-id", default=None, help="optional ID for benchmark logs")
    parser.add_argument("--fail-first", help="inject a transient replay failure at YYYY-MM-DD:metric")
    args = parser.parse_args()

    if args.snapshot and (args.start_date or args.end_date):
        parser.error("a single snapshot supplies its own date")
    if (args.start_date is None) != (args.end_date is None):
        parser.error("provide both --start-date and --end-date")
    if args.start_date is not None:
        if args.end_date <= args.start_date:
            parser.error("--end-date must be later than --start-date")
        if args.end_date > datetime.now(UTC).date():
            parser.error("choose completed UTC days")
    if os.environ.get("RENEWFLOW_BENCHMARK_SCHEMA"):
        parser.error("use the authority benchmark runner for trial schemas")
    if args.manifest and args.fail_first:
        parser.error("--manifest uses the application database without fault injection")

    current_day = None
    run_id = args.run_id or f"python-{uuid4().hex}"
    try:
        if args.manifest:
            with connect_observations() as connection:
                rejected = load_manifest(connection, args.manifest, args.start_date, args.end_date)
            if rejected:
                parser.exit(1, "Some authority-days failed validation. See the load summary.\n")
            return

        fault = replay_fault(args.fail_first)
        snapshots = {}
        if args.snapshot:
            day, _ = read_snapshot(args.snapshot)
            snapshots = {day: args.snapshot}
        elif args.replay_manifest:
            snapshots = read_manifest(args.replay_manifest)
        if args.start_date is not None:
            starts = range((args.end_date - args.start_date).days)
            dates = [args.start_date + timedelta(days=offset) for offset in starts]
        else:
            dates = sorted(snapshots)
        if (missing := next((day for day in dates if day not in snapshots), None)):
            raise ValueError(f"replay manifest has no snapshot for {missing}")
        if fault and fault[0] not in dates:
            raise ValueError("fail_first must target a date in the replay")

        with record_attempt(print, runner="python", run_id=run_id, stage="run") as run_counts:
            run_counts["days_requested"] = len(dates)
            run_counts["days_published"] = 0
            with connect_observations() as connection:
                for current_day in dates:
                    results = [
                        _extract(current_day, metric, snapshots[current_day], fault, run_id)
                        for metric in SERIES
                    ]
                    batch = pd.concat([result.observations for result in results], ignore_index=True)
                    with record_attempt(
                        print, runner="python", run_id=run_id, stage="load_day", day=current_day,
                    ) as counts:
                        counts["rows_validated"] = len(batch)
                        status, changed_rows = publish_day(connection, batch)
                        counts["days_skipped"] = int(status == "skipped")
                        counts["rows_changed"] = changed_rows
                    run_counts["days_published"] += 1
                    print(
                        f"{current_day} UTC | validated {len(batch)} observations | "
                        f"inserted or updated {changed_rows} | "
                        f"duplicates collapsed {sum(result.identical_duplicates for result in results)} | "
                        f"negative generation hours {sum(result.negative_generation_hours for result in results)} | {status}"
                    )
    except psycopg.Error as error:
        day = f" for {current_day} UTC" if current_day else ""
        code = f" (SQLSTATE {error.sqlstate})" if error.sqlstate else ""
        parser.exit(1, f"PostgreSQL operation failed{day}{code}. Check the connection and schema.\n")
    except (InjectedExtractFailure, KeyError, OSError, TypeError, ValueError) as error:
        parser.exit(1, f"{error}\n")

    print(f"PostgreSQL replay complete. Verified {len(dates)} UTC day(s).")


if __name__ == "__main__":
    main()
