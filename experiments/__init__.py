"""Reproducible CSV preparation and local benchmark commands."""

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
