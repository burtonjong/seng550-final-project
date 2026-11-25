"""Replay one captured EIA authority over a benchmark window."""

import argparse
import json
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg

from renewflow.db import connect_observations, load_day
from renewflow.eia import SERIES
from renewflow.pipeline import record_attempt
from renewflow.replay import read_manifest, read_snapshot, replay_path
from renewflow.transform import normalize_series, normalize_subregional_demand

AUTHORITIES = ("CISO", "ERCO", "MISO", "PJM", "NYIS", "ISNE", "SWPP")
COHORTS = (7, 30, 90, 365)
START_DATE = date(2025, 1, 15)
BATCH_DAG = "renewflow_authority_batches"
BATCH_TASKS = tuple(f"process_authority_{index}" for index in range(4))
SUBREGION_CODE = re.compile(r"[A-Z0-9]{2,4}")


@dataclass(frozen=True)
class BatchReplay:
    manifests: dict[str, Path]
    cohorts: tuple[int, ...]
    subregions: dict[str, tuple[str, ...]] = field(default_factory=dict)


def read_batch_replay(path: Path) -> BatchReplay:
    data = json.loads(path.read_text(encoding="utf-8"))
    cohorts = tuple(data["cohorts"])
    selected = data["authorities"]
    if not cohorts:
        raise ValueError("capture coverage is insufficient for the batch experiment")
    if (
        data["start_date"] != START_DATE.isoformat()
        or len(cohorts) < 2
        or cohorts != COHORTS[:len(cohorts)]
        or len(selected) != len(BATCH_TASKS)
        or tuple(selected) != tuple(authority for authority in AUTHORITIES if authority in selected)
        or selected[0] != "CISO"
    ):
        raise ValueError("invalid authority selection or UTC cohorts in the batch manifest")
    manifests = {
        authority: replay_path(path.parent, data["authority_manifests"][authority]) for authority in selected
    }
    subregions = {}
    if "subregions" in data:
        configured = data["subregions"]
        if not isinstance(configured, dict) or configured.keys() != manifests.keys():
            raise ValueError("the batch manifest must list subregions for every authority")
        for authority, codes in configured.items():
            if (
                not isinstance(codes, list) or not codes
                or not all(isinstance(code, str) and SUBREGION_CODE.fullmatch(code) for code in codes)
                or codes != sorted(set(codes))
            ):
                raise ValueError(f"invalid subregion list for {authority}")
            subregions[authority] = tuple(codes)
    return BatchReplay(manifests, cohorts, subregions)


def batch_snapshots(manifest: Path, days: int) -> Iterator[tuple[date, Path]]:
    snapshots = read_manifest(manifest)
    for offset in range(days):
        day = START_DATE + timedelta(days=offset)
        if day not in snapshots:
            raise ValueError(f"{manifest.parent.name} has no snapshot for {day}")
        yield day, snapshots[day]


def normalize_snapshot(path: Path, day: date, authority: str) -> pd.DataFrame:
    observations, _ = normalize_batch_snapshot(path, day, authority, ())
    return observations


def normalize_batch_snapshot(
    path: Path, day: date, authority: str, subregions: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    captured_day, rows = read_snapshot(path, include_subregions=bool(subregions))
    if captured_day != day:
        raise ValueError(f"{path.name} describes {captured_day}, expected {day}")
    observations = pd.concat(
        [normalize_series(metric, day, rows[metric], respondent=authority).observations for metric in SERIES],
        ignore_index=True,
    )
    subregional = normalize_subregional_demand(
        day, rows["subregional_demand"], respondent=authority, subregions=subregions,
    ) if subregions else None
    return observations, subregional


def load_authority_batch(
    manifest: Path, authority: str, days: int, *, schema: str, subregions: tuple[str, ...] = (),
) -> dict[str, int]:
    if not schema:
        raise ValueError("the authority batch requires an isolated benchmark schema")
    rows_validated = rows_changed = 0
    with connect_observations(benchmark_schema=schema) as connection:
        for day, snapshot in batch_snapshots(manifest, days):
            observations, subregional = normalize_batch_snapshot(snapshot, day, authority, subregions)
            rows_validated += len(observations) + (len(subregional) if subregional is not None else 0)
            rows_changed += load_day(connection, observations, subregional=subregional)
    return {"rows_validated": rows_validated, "rows_changed": rows_changed}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--days", type=int, choices=COHORTS, required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        replay = read_batch_replay(args.manifest)
        if args.days not in replay.cohorts:
            raise ValueError(f"capture does not cover the {args.days}-day cohort")
        schema = os.environ.get("RENEWFLOW_BENCHMARK_SCHEMA", "")
        for stage, (authority, manifest) in zip(BATCH_TASKS, replay.manifests.items(), strict=True):
            with record_attempt(print, runner="python", run_id=args.run_id, stage=stage) as counts:
                counts.update(load_authority_batch(
                    manifest, authority, args.days, schema=schema, subregions=replay.subregions.get(authority, ()),
                ))
    except psycopg.Error as error:
        parser.exit(1, f"PostgreSQL batch load failed ({type(error).__name__}).\n")
    except (KeyError, OSError, TypeError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
