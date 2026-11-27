"""Read local EIA-930 Balance and Subregion CSVs for both runners."""

import csv
import hashlib
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from renewflow.eia import SERIES
from renewflow.replay import replay_path
from renewflow.transform import _parse_mwh

AUTHORITY = "Balancing Authority"
HOUR_END = "UTC Time at End of Hour"
ZONE = "Sub-Region"
DEMAND = "Demand (MW)"
FUEL = "Net Generation (MW) from {} {} Integrated Battery Storage"
COLUMNS = {metric: FUEL.format(metric.title(), "without") for metric in ("solar", "wind")}
COLUMNS["demand"] = DEMAND
BATTERY = tuple(FUEL.format(metric.title(), "with") for metric in ("solar", "wind"))
ZONES = {
    "CISO": ("PGAE", "SCE", "SDGE", "VEA"),
    "ERCO": ("EAST", "NCEN", "NRTH", "SCEN", "SOUT", "WEST"),
    "MISO": ("0001", "0004", "0006", "0027", "0035", "8910"),
    "PJM": ("AE", "AEP", "AP", "ATSI", "BC", "CE", "DAY", "DEOK", "DOM", "DPL", "DUQ",
            "EKPC", "JC", "ME", "PE", "PEP", "PL", "PN", "PS", "RECO"),
}
ZONE_SERIES = "subregional_demand"
ONE_HOUR = timedelta(hours=1)


def describe_source(path: Path) -> dict:
    with path.open("rb") as source:
        return {
            "filename": path.name, "bytes": path.stat().st_size,
            "sha256": hashlib.file_digest(source, "sha256").hexdigest(),
        }


def csv_paths(sources: tuple[dict, ...]) -> tuple[Path, ...]:
    root = Path(os.environ.get("RENEWFLOW_CSV_DIR") or "data")
    paths = tuple(replay_path(root, source["filename"]) for source in sources)
    for path, source in zip(paths, sources, strict=True):
        if path.stat().st_size != source["bytes"]:
            raise ValueError(f"{source['filename']}: CSV size differs from the benchmark manifest")
    return paths


def verify_sources(sources: tuple[dict, ...]) -> None:
    for path, source in zip(csv_paths(sources), sources, strict=True):
        if describe_source(path)["sha256"] != source["sha256"]:
            raise ValueError(f"{source['filename']}: CSV checksum differs from the benchmark manifest")


def collect_rows(
    paths: tuple[Path, ...], start: date, end: date, *,
    zones: dict[str, tuple[str, ...]] = ZONES, dates: set[date] | None = None,
) -> dict:
    """Scan both CSV families and retain only the selected authority, zone and UTC keys."""
    records = defaultdict(dict)
    kinds = set()
    for path in paths:
        with path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source)
            header = next(reader, [])
            columns = {name: index for index, name in enumerate(header)}
            is_zone = ZONE in columns
            balance = {
                metric: column if column in columns else column.removesuffix(" without Integrated Battery Storage")
                for metric, column in COLUMNS.items()
            }
            required = {AUTHORITY, HOUR_END, ZONE, DEMAND} if is_zone else {AUTHORITY, HOUR_END, *balance.values()}
            if not required <= columns.keys() or len(columns) != len(header):
                raise ValueError(f"{path.name}: unrecognized EIA-930 CSV header")
            kind = "subregion" if is_zone else "balance"
            kinds.add(kind)
            for row in reader:
                if not row:
                    continue
                location = f"{path.name}:{reader.line_num}"
                if len(row) != len(header):
                    raise ValueError(f"{location}: unexpected column count")
                authority = row[columns[AUTHORITY]].strip()
                if authority not in zones:
                    continue
                zone = row[columns[ZONE]].strip() if is_zone else None
                if is_zone and zone not in zones[authority]:
                    continue
                try:
                    hour_end = datetime.strptime(row[columns[HOUR_END]], "%m/%d/%Y %I:%M:%S %p")
                    if hour_end.minute or hour_end.second:
                        raise ValueError
                except ValueError:
                    raise ValueError(f"{location}: invalid UTC hour ending for {authority}") from None
                day = (hour_end - ONE_HOUR).date()
                if not start <= day < end or (dates is not None and day not in dates):
                    continue
                period = hour_end.isoformat(timespec="hours")
                for column in BATTERY if not is_zone else ():
                    if column in columns:
                        value = row[columns[column]].strip()
                        if value and _parse_mwh(value) != 0:
                            raise ValueError(f"{location}: unsupported integrated-storage value for {authority} {period}")
                selected = {zone: DEMAND} if is_zone else balance
                for code, column in selected.items():
                    value = row[columns[column]].strip()
                    key = (code, period)
                    daily = records[authority, day]
                    if key in daily:
                        previous = daily[key]
                        numeric = _parse_mwh(value)
                        if value != previous and (numeric is None or numeric != _parse_mwh(previous)):
                            raise ValueError(f"{location}: conflicting overlap for {authority} {day} {code} {period}")
                    else:
                        daily[key] = value
    if kinds != {"balance", "subregion"} or not records:
        raise ValueError("provide both Balance and Subregion CSVs containing data in the requested range")
    return records


def day_payload(day: date, authority: str, values: dict) -> dict:
    rows = defaultdict(list)
    zones = []
    for (code, period), value in sorted(values.items()):
        row = {"period": period, "value": value, "value-units": "megawatthours"}
        if code in SERIES:
            identifier, identifier_value = SERIES[code]
            rows[code].append(row | {"respondent": authority, identifier: identifier_value})
        else:
            zones.append(row | {"parent": authority, "subba": code})

    return {
        "utc_date": str(day), "frequency": "hourly",
        "series": {metric: {"response": {"data": rows[metric]}} for metric in SERIES},
        ZONE_SERIES: {"response": {"data": zones}},
    }
