"""Reproducible EIA capture and local benchmark commands."""

from datetime import UTC, date, datetime, timedelta

STAGED_EXPERIMENT = "staged_authority_batches"
CSV_EXPERIMENT = "direct_csv_authority_batches"


def input_identity(inputs: dict) -> tuple:
    """Identify matched benchmark inputs across trials and archived protocols."""
    if "source_files" in inputs:
        return (
            tuple(inputs["utc_dates"]),
            tuple((source["filename"], source["sha256"]) for source in inputs["source_files"]),
            tuple(sorted((authority, tuple(zones)) for authority, zones in inputs["subregions"].items())),
        )
    return tuple(sorted((day, item["sha256"]) for day, item in inputs["captures"].items()))


def utc_days(start: date, end: date) -> list[date]:
    if start >= end or end > datetime.now(UTC).date():
        raise ValueError("choose a nonempty range of completed UTC days")
    return [start + timedelta(days=index) for index in range((end - start).days)]
