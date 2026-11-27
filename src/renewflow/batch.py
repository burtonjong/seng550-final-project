"""Run the same staged EIA replay in sequential Python and Airflow."""

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import sleep

import pandas as pd
import psycopg

from renewflow.db import connect_observations, load_day
from renewflow.eia import SERIES
from renewflow.eia930_csv import ZONES, collect_rows, csv_paths, day_payload
from renewflow.pipeline import record_attempt
from renewflow.replay import read_snapshot, replay_path
from renewflow.transform import normalize_series, normalize_subregional_demand

AUTHORITIES = tuple(ZONES)
COHORTS = (1, 7, 30)
START_DATE = date(2024, 1, 1)
BATCH_DAG = "renewflow_authority_batches"
AUTHORITY_COUNT = len(AUTHORITIES)
STAGES = ("extract", "transform", "load")
BATCH_TASKS = tuple(f"{stage}_authority_{index}" for index in range(AUTHORITY_COUNT) for stage in STAGES)
STAGING_ROOT = Path(".renewflow/staging")
LOAD_RETRIES = 1
LOAD_RETRY_DELAY = 5
FAULT_AFTER_DAYS = 3
SUBREGION_CODE = re.compile(r"[A-Z0-9]{2,4}")


@dataclass(frozen=True)
class BatchReplay:
    cohorts: tuple[int, ...]
    subregions: dict[str, tuple[str, ...]]
    dates: tuple[date, ...]
    csv_sources: tuple[dict, ...]

    @property
    def authorities(self) -> tuple[str, ...]:
        return tuple(self.subregions)

    def selected_dates(self, days: int) -> tuple[date, ...]:
        if not 0 < days <= len(self.dates):
            raise ValueError("requested days exceed replay coverage")
        return self.dates[:days]


def replay_cohorts(available: int, extra: tuple[int, ...] = ()) -> tuple[int, ...]:
    if any(type(size) is not int or not 1 <= size <= available for size in extra):
        raise ValueError(f"extra cohorts must be between 1 and {available} complete UTC days")
    return tuple(sorted({size for size in (*COHORTS, available, *extra) if 0 < size <= available}))


def read_batch_replay(path: Path) -> BatchReplay:
    data = json.loads(path.read_text(encoding="utf-8"))
    cohorts = tuple(data["cohorts"])
    selected = data["authorities"]
    version = data.get("version")
    if version != 2:
        raise ValueError("unsupported CSV replay manifest version")
    dates = tuple(date.fromisoformat(label) for label in data["utc_dates"])
    extra = tuple(data.get("extra_cohorts", ()))
    if (
        not dates or dates != tuple(sorted(set(dates)))
        or cohorts != replay_cohorts(len(dates), extra) or tuple(selected) != AUTHORITIES
        or data["start_date"] != dates[0].isoformat() or "subregions" not in data
    ):
        raise ValueError("invalid CSV replay dates, cohorts, or authorities")
    sources = tuple(data["source_files"])
    filenames = set()
    for source in sources:
        name = source["filename"]
        if (
            not name.endswith(".csv") or name != Path(name).name or name in filenames
            or type(source["bytes"]) is not int or source["bytes"] <= 0
            or not re.fullmatch(r"[0-9a-f]{64}", source["sha256"])
        ):
            raise ValueError("invalid benchmark CSV source metadata")
        filenames.add(name)
    if len(sources) < 2:
        raise ValueError("the benchmark manifest must list Balance and Subregion CSV files")
    subregions = {}
    configured = data["subregions"]
    if not isinstance(configured, dict) or configured.keys() != set(selected):
        raise ValueError("the batch manifest must list subregions for every authority")
    for authority in selected:
        codes = configured[authority]
        if (
            not isinstance(codes, list) or not codes
            or not all(isinstance(code, str) and SUBREGION_CODE.fullmatch(code) for code in codes)
            or codes != sorted(set(codes))
        ):
            raise ValueError(f"invalid subregion list for {authority}")
        subregions[authority] = tuple(codes)
    return BatchReplay(cohorts, subregions, dates, sources)


def normalize_batch_snapshot(
    path: Path, day: date, authority: str, subregions: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    captured_day, rows = read_snapshot(path, include_subregions=bool(subregions))
    if captured_day != day:
        raise ValueError(f"{path.name} describes {captured_day}, expected {day}")
    return normalize_batch_rows(rows, day, authority, subregions)


def normalize_batch_rows(
    rows: dict, day: date, authority: str, subregions: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    observations = pd.concat(
        [normalize_series(metric, day, rows[metric], respondent=authority).observations for metric in SERIES],
        ignore_index=True,
    )
    subregional = normalize_subregional_demand(
        day, rows["subregional_demand"], respondent=authority, subregions=subregions,
    ) if subregions else None
    return observations, subregional


class InjectedLoadFailure(RuntimeError):
    """A transient error injected after a committed authority-day."""


@dataclass(frozen=True)
class AuthorityBatch:
    authority: str
    index: int
    dates: tuple[date, ...]
    subregions: tuple[str, ...]
    schema: str
    work: Path
    fault: bool
    csv_sources: tuple[dict, ...] = ()


def trial_work(schema: str) -> Path:
    if not isinstance(schema, str) or not re.fullmatch(r"trial_[0-9a-f]{32}", schema):
        raise ValueError("staged replay requires a trial schema")
    return replay_path(Path(os.environ.get("RENEWFLOW_STAGING_DIR") or STAGING_ROOT), schema)


def authority_batch(replay: BatchReplay, index: int, days: int, schema: str, fault: bool) -> AuthorityBatch:
    if type(days) is not int or days not in replay.cohorts:
        raise ValueError("requested days are outside the captured cohorts")
    if type(fault) is not bool or (fault and days <= FAULT_AFTER_DAYS):
        raise ValueError("fault injection requires a boolean and at least four days")
    authority = replay.authorities[index]
    work = replay_path(trial_work(schema), authority)
    return AuthorityBatch(
        authority, index, replay.selected_dates(days),
        replay.subregions.get(authority, ()), schema, work, fault, replay.csv_sources,
    )


def _atomic_write(path: Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        pending = Path(temporary.name)
        try:
            temporary.write(contents)
            temporary.flush()
            pending.chmod(0o644)
            pending.replace(path)
        finally:
            pending.unlink(missing_ok=True)


def _encode(frame: pd.DataFrame) -> list[list[str]]:
    return [
        [authority, hour.isoformat(), series, str(value)]
        for authority, hour, series, value in frame.itertuples(index=False, name=None)
    ]


def _decode(rows: list, series: str) -> pd.DataFrame:
    return pd.DataFrame(
        [(authority, pd.Timestamp(hour), code, Decimal(value)) for authority, hour, code, value in rows],
        columns=["respondent", "hour_start_utc", series, "value_mwh"],
    )


def extract_authority(batch: AuthorityBatch, counts: dict[str, int]) -> str:
    paths = csv_paths(batch.csv_sources)
    first, last = batch.dates[0], batch.dates[-1]
    records = collect_rows(
        paths, first, last + timedelta(days=1),
        zones={batch.authority: batch.subregions}, dates=set(batch.dates),
    )
    counts["csv_bytes_scanned"] = sum(source["bytes"] for source in batch.csv_sources)
    for day in batch.dates:
        values = records.get((batch.authority, day), {})
        payload = day_payload(day, batch.authority, values)
        _atomic_write(batch.work / "raw" / f"{day}.json", json.dumps(payload).encode())
        counts["rows_processed"] += len(values)
        counts["days_processed"] += 1
    return "raw"


def transform_authority(batch: AuthorityBatch, source: str | None, counts: dict[str, int]) -> str:
    if source != "raw":
        raise ValueError("transform requires the raw staging reference")
    for day in batch.dates:
        observations, subregional = normalize_batch_snapshot(
            batch.work / source / f"{day}.json", day, batch.authority, batch.subregions,
        )
        payload = {"observations": _encode(observations)}
        if subregional is not None:
            payload["subregional"] = _encode(subregional)
        _atomic_write(batch.work / "normalized" / f"{day}.json", json.dumps(payload).encode())
        counts["rows_processed"] += sum(len(rows) for rows in payload.values())
        counts["days_processed"] += 1
    return "normalized"


def load_authority(batch: AuthorityBatch, source: str | None, counts: dict[str, int], attempt: int) -> None:
    if source != "normalized":
        raise ValueError("load requires the normalized staging reference")
    counts.update(rows_changed=0, days_committed=0)
    with connect_observations(benchmark_schema=batch.schema) as connection:
        for day in batch.dates:
            payload = json.loads((batch.work / source / f"{day}.json").read_bytes())
            observations = _decode(payload["observations"], "metric")
            subregional = _decode(payload["subregional"], "subregion") if "subregional" in payload else None
            counts["rows_processed"] += sum(len(rows) for rows in payload.values())
            counts["days_processed"] += 1
            counts["rows_changed"] += load_day(connection, observations, subregional=subregional)
            counts["days_committed"] += 1
            if batch.fault and batch.authority == "CISO" and attempt == 1 and counts["days_committed"] == FAULT_AFTER_DAYS:
                raise InjectedLoadFailure("injected transient failure after the third CISO day committed")


def execute_stage(batch: AuthorityBatch, stage: str, source: str | None, *, runner: str, attempt: int, emit):
    """Run and record one stage. Only small staging references cross task boundaries."""
    task_id = f"{stage}_authority_{batch.index}"

    def record(message: str) -> None:
        _, _, payload = message.partition(" ")
        event = json.loads(payload)
        if event["status"] != "started":
            event["authority"] = batch.authority
            _atomic_write(batch.work / "attempts" / f"{stage}-{attempt}.json", json.dumps(event).encode())
        emit(message)

    with record_attempt(record, runner=runner, run_id=batch.schema.removeprefix("trial_"), stage=task_id,
                        attempt=attempt) as counts:
        counts.update(rows_processed=0, days_processed=0)
        if stage == "extract":
            return extract_authority(batch, counts)
        if stage == "transform":
            return transform_authority(batch, source, counts)
        if stage == "load":
            return load_authority(batch, source, counts, attempt)
        raise ValueError(f"unknown ETL stage {stage}")


def read_attempts(work: Path) -> list[dict]:
    events = [json.loads(path.read_bytes()) for path in work.glob("*/attempts/*.json")]
    return sorted(events, key=lambda event: (event["started_at"], event["stage"], event["attempt"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--inject-load-failure", action="store_true")
    args = parser.parse_args()
    try:
        replay = read_batch_replay(args.manifest)
        schema = os.environ.get("RENEWFLOW_BENCHMARK_SCHEMA", "")
        if schema != f"trial_{args.run_id}":
            raise ValueError("run ID must match the isolated benchmark schema")
        for index in range(len(replay.authorities)):
            batch = authority_batch(replay, index, args.days, schema, args.inject_load_failure)
            source = None
            for stage in STAGES:
                for attempt in range(1, LOAD_RETRIES + 2):
                    try:
                        source = execute_stage(batch, stage, source, runner="python", attempt=attempt, emit=print)
                        break
                    except InjectedLoadFailure:
                        if attempt > LOAD_RETRIES:
                            raise
                        sleep(LOAD_RETRY_DELAY)
    except psycopg.Error as error:
        parser.exit(1, f"PostgreSQL batch load failed ({type(error).__name__}).\n")
    except (InjectedLoadFailure, KeyError, OSError, TypeError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
