"""Read staged hourly records and resolve benchmark paths."""

import json
from datetime import date
from pathlib import Path

from renewflow.eia import SERIES


def read_snapshot(
    path: Path, *, include_subregions: bool = False
) -> tuple[date, dict[str, list[dict[str, object]]]]:
    try:
        capture = json.loads(path.read_text(encoding="utf-8"))
        day = date.fromisoformat(capture["utc_date"])
        rows = snapshot_rows(capture, include_subregions=include_subregions)
    except (AttributeError, KeyError, TypeError, ValueError):
        required = "all three series and subregional demand" if include_subregions else "all three series"
        raise ValueError(f"{path.name} must contain an hourly EIA snapshot with {required}") from None
    return day, rows


def snapshot_rows(capture: dict, *, include_subregions: bool) -> dict:
    if capture["frequency"] != "hourly" or capture["series"].keys() != SERIES.keys():
        raise ValueError
    rows = {metric: capture["series"][metric]["response"]["data"] for metric in SERIES}
    if include_subregions:
        rows["subregional_demand"] = capture["subregional_demand"]["response"]["data"]
    if any(not isinstance(records, list) for records in rows.values()):
        raise ValueError
    return rows


def replay_path(root: Path, filename: str) -> Path:
    """Resolve a relative file inside the replay directory."""
    path = Path(filename)
    resolved = (root / path).resolve()
    if path.is_absolute() or not resolved.is_relative_to(root.resolve()):
        raise ValueError("replay files must be relative paths inside the replay directory")
    return resolved
