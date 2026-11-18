"""Run RenewFlow sequentially from a saved EIA snapshot or a live UTC date range."""

import argparse
import json
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg
import requests

from renewflow.db import load_day
from renewflow.eia import SERIES, EIAError, fetch_series
from renewflow.transform import normalize_series


def _snapshot_day(path: Path) -> tuple[date, dict[str, list[dict[str, object]]]]:
    try:
        capture = json.loads(path.read_text(encoding="utf-8"))
        utc_date = date.fromisoformat(capture["utc_date"])
        if capture["frequency"] != "hourly" or capture["series"].keys() != SERIES.keys():
            raise ValueError
        rows = {metric: capture["series"][metric]["response"]["data"] for metric in SERIES}
        if any(not isinstance(records, list) for records in rows.values()):
            raise ValueError
    except (AttributeError, KeyError, TypeError, ValueError):
        raise ValueError(f"{path} must contain an hourly EIA snapshot with all three series") from None
    return utc_date, rows


def _live_days(
    session: requests.Session, start: date, end: date, api_key: str
) -> Iterator[tuple[date, dict[str, list[dict[str, object]]]]]:
    day = start
    while day < end:
        rows = {}
        for metric in SERIES:
            try:
                rows[metric] = fetch_series(session, metric, day, api_key=api_key).rows
            except EIAError as error:
                raise EIAError(f"{day} UTC {metric}: {error}") from None
        yield day, rows
        day += timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--snapshot", type=Path, help="load one day from a sanitized EIA snapshot")
    source.add_argument("--start-date", type=date.fromisoformat, help="first UTC date to fetch")
    parser.add_argument("--end-date", type=date.fromisoformat, help="exclusive end of the UTC range")
    args = parser.parse_args()

    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if args.snapshot and args.end_date is not None:
        parser.error("--end-date is only used with --start-date")
    if args.start_date is not None:
        if args.end_date is None or args.end_date <= args.start_date:
            parser.error("--end-date must be later than --start-date")
        if args.end_date > datetime.now(UTC).date():
            parser.error("choose completed UTC days")
        if not api_key:
            parser.error("set EIA_API_KEY to fetch live data")

    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        parser.error("set POSTGRES_PASSWORD in .env and load it with uv --env-file .env")

    current_day = None
    loaded_days = 0
    try:
        snapshot = _snapshot_day(args.snapshot) if args.snapshot else None
        with (
            requests.Session() as session,
            psycopg.connect(
                host=os.environ.get("POSTGRES_HOST") or "127.0.0.1",
                port=os.environ.get("POSTGRES_PORT") or "5432",
                dbname=os.environ.get("POSTGRES_DB") or "renewflow",
                user=os.environ.get("POSTGRES_USER") or "renewflow",
                password=password,
                connect_timeout=5,
                autocommit=True,
            ) as connection,
        ):
            days = (
                (snapshot,) if snapshot else _live_days(session, args.start_date, args.end_date, api_key)
            )
            for current_day, raw_rows in days:
                results = [normalize_series(metric, current_day, raw_rows[metric]) for metric in SERIES]
                batch = pd.concat([result.observations for result in results], ignore_index=True)
                changed_rows = load_day(connection, batch)
                loaded_days += 1
                print(
                    f"{current_day} UTC | validated {len(batch)} observations | "
                    f"inserted or updated {changed_rows} | "
                    f"duplicates collapsed {sum(result.identical_duplicates for result in results)} | "
                    "negative generation hours "
                    f"{sum(result.negative_generation_hours for result in results)}"
                )
    except psycopg.Error as error:
        day = f" for {current_day} UTC" if current_day else ""
        code = f" (SQLSTATE {error.sqlstate})" if error.sqlstate else ""
        parser.exit(1, f"PostgreSQL operation failed{day}{code}. Check the connection and schema.\n")
    except (EIAError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")

    print(f"PostgreSQL load complete. Committed {loaded_days} UTC day(s).")


if __name__ == "__main__":
    main()
