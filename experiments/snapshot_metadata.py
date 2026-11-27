"""Measure the frozen files used as benchmark inputs."""

import hashlib
import json
from datetime import date
from pathlib import Path

from renewflow.eia import RESPONDENT, SERIES
from renewflow.replay import read_snapshot
from renewflow.transform import normalize_series


def snapshot_metadata(path: Path, day: date, *, respondent: str = RESPONDENT) -> dict:
    captured_day, rows = read_snapshot(path)
    if captured_day != day:
        raise ValueError(f"{path.name} contains the wrong UTC date")
    records = sum(
        len(normalize_series(metric, day, rows[metric], respondent=respondent).observations) for metric in SERIES
    )
    return snapshot_file_metadata(path.read_bytes(), records)


def snapshot_file_metadata(raw: bytes, observations: int) -> dict:
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
    }
