"""Run RenewFlow sequentially from a snapshot, replay manifest, or live UTC range."""

import argparse
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import sleep
from uuid import uuid4

import pandas as pd
import psycopg
import requests

from renewflow.db import connect_observations, load_day
from renewflow.eia import SERIES, EIAError, RetryableEIAError, fetch_series
from renewflow.pipeline import (
    EXTRACT_RETRIES,
    RETRY_DELAY_SECONDS,
    fail_on_first_attempt,
    record_attempt,
    replay_fault,
)
from renewflow.replay import read_manifest, read_series, read_snapshot
from renewflow.transform import normalize_series


def _extract(session, day, metric, snapshot, api_key, fault, run_id):
    for attempt in range(1, EXTRACT_RETRIES + 2):
        try:
            with record_attempt(
                print, runner="python", run_id=run_id, stage=f"extract_{metric}",
                day=day, attempt=attempt,
            ) as counts:
                fail_on_first_attempt(fault, day, metric, attempt)
                if snapshot is not None:
                    rows = read_series(snapshot, day, metric)
                else:
                    rows = fetch_series(session, metric, day, api_key=api_key).rows
                counts["rows_received"] = len(rows)
                result = normalize_series(metric, day, rows)
                counts["rows_validated"] = len(result.observations)
                counts["identical_duplicates"] = result.identical_duplicates
                counts["negative_generation_hours"] = result.negative_generation_hours
                return result
        except RetryableEIAError as error:
            if attempt > EXTRACT_RETRIES:
                raise EIAError(f"{day} UTC {metric}: {error}") from None
            sleep(RETRY_DELAY_SECONDS)
        except EIAError as error:
            raise EIAError(f"{day} UTC {metric}: {error}") from None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
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
    elif not (args.snapshot or args.replay_manifest):
        parser.error("provide a snapshot, replay manifest, or UTC date range")

    replay = bool(args.snapshot or args.replay_manifest)
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not replay and not api_key:
        parser.error("set EIA_API_KEY to fetch live data")

    current_day = None
    run_id = args.run_id or f"python-{uuid4().hex}"
    try:
        fault = replay_fault(args.fail_first, replay=replay)
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
        if replay and (missing := next((day for day in dates if day not in snapshots), None)):
            raise ValueError(f"replay manifest has no snapshot for {missing}")
        if fault and fault[0] not in dates:
            raise ValueError("fail_first must target a date in the replay")

        with record_attempt(print, runner="python", run_id=run_id, stage="run") as run_counts:
            run_counts["days_requested"] = len(dates)
            run_counts["days_published"] = 0
            with (
                requests.Session() as session,
                connect_observations(benchmark_schema=os.environ.get("RENEWFLOW_BENCHMARK_SCHEMA")) as connection,
            ):
                for current_day in dates:
                    results = [
                        _extract(session, current_day, metric, snapshots.get(current_day), api_key, fault, run_id)
                        for metric in SERIES
                    ]
                    batch = pd.concat([result.observations for result in results], ignore_index=True)
                    with record_attempt(
                        print, runner="python", run_id=run_id, stage="load_day", day=current_day,
                    ) as counts:
                        counts["rows_validated"] = len(batch)
                        changed_rows = load_day(connection, batch)
                        counts["rows_changed"] = changed_rows
                    run_counts["days_published"] += 1
                    print(
                        f"{current_day} UTC | validated {len(batch)} observations | "
                        f"inserted or updated {changed_rows} | "
                        f"duplicates collapsed {sum(result.identical_duplicates for result in results)} | "
                        f"negative generation hours {sum(result.negative_generation_hours for result in results)}"
                    )
    except psycopg.Error as error:
        day = f" for {current_day} UTC" if current_day else ""
        code = f" (SQLSTATE {error.sqlstate})" if error.sqlstate else ""
        parser.exit(1, f"PostgreSQL operation failed{day}{code}. Check the connection and schema.\n")
    except (EIAError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")

    print(f"PostgreSQL load complete. Committed {len(dates)} UTC day(s).")


if __name__ == "__main__":
    main()
