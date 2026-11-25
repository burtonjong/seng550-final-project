"""Validate and normalize EIA observations for one authority, series, and UTC day."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd

from renewflow.eia import RESPONDENT, SERIES


class ValidationError(ValueError):
    """The EIA rows cannot be used to publish the requested series and day."""


@dataclass
class NormalizedSeries:
    observations: pd.DataFrame
    rows_received: int
    identical_duplicates: int
    negative_generation_hours: int


def _parse_mwh(value: object) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    text = str(value).strip()
    if "_" in text:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _hourly_values(frame: pd.DataFrame, utc_date: date, context: str):
    wrong_units = ~frame["value-units"].eq("megawatthours")
    if wrong_units.any():
        raise ValidationError(f"{context}: {wrong_units.sum()} missing or invalid units")

    periods = frame["period"].astype("string")
    hourly_labels = periods.str.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}", na=False)
    hour_ends = pd.to_datetime(
        periods.where(hourly_labels), format="%Y-%m-%dT%H", errors="coerce", utc=True
    )
    if hour_ends.isna().any():
        raise ValidationError(f"{context}: {hour_ends.isna().sum()} missing or invalid timestamps")

    # The midnight hour ending belongs to the previous UTC operating day.
    hour_starts = hour_ends - pd.Timedelta(hours=1)
    expected = pd.date_range(pd.Timestamp(utc_date, tz="UTC"), periods=24, freq="h")
    outside = ~hour_starts.isin(expected)
    if outside.any():
        raise ValidationError(f"{context}: {outside.sum()} timestamps outside the UTC day")

    values = frame["value"].map(_parse_mwh)
    if values.isna().any():
        raise ValidationError(
            f"{context}: {values.isna().sum()} missing, nonnumeric, or nonfinite values"
        )
    return hour_starts, expected, values


def normalize_series(
    metric: str, utc_date: date, rows: list[dict[str, object]], *, respondent: str = RESPONDENT
) -> NormalizedSeries:
    """Return 24 unique, sorted hourly observations or raise ValidationError."""
    if metric not in SERIES:
        raise ValueError("metric must be solar, wind, or demand")
    _, facet, code = SERIES[metric]
    context = f"{utc_date} {respondent} {metric}"

    invalid_records = sum(not isinstance(row, dict) for row in rows)
    if invalid_records:
        raise ValidationError(f"{context}: {invalid_records} invalid records")
    frame = pd.DataFrame(
        rows, columns=["respondent", facet, "value-units", "period", "value"], dtype=object
    )
    wrong_series = ~(frame["respondent"].eq(respondent) & frame[facet].eq(code))
    if wrong_series.any():
        raise ValidationError(f"{context}: {wrong_series.sum()} mismatched series identifiers")
    hour_starts, expected, values = _hourly_values(frame, utc_date, context)
    if metric == "demand":
        nonpositive = (values <= 0).sum()
        if nonpositive:
            raise ValidationError(f"{context}: {nonpositive} nonpositive demand values")

    observations = pd.DataFrame(
        {
            "respondent": respondent,
            "hour_start_utc": hour_starts,
            "metric": metric,
            "value_mwh": values,
        }
    ).drop_duplicates()
    conflicts = observations.duplicated("hour_start_utc", keep=False)
    if conflicts.any():
        count = observations.loc[conflicts, "hour_start_utc"].nunique()
        raise ValidationError(f"{context}: {count} hours with conflicting duplicate values")
    missing = expected.difference(observations["hour_start_utc"])
    if len(missing):
        hours = ", ".join(missing.strftime("%Y-%m-%dT%H:%MZ"))
        raise ValidationError(f"{context}: {len(missing)} missing hours ({hours})")

    negative_hours = 0 if metric == "demand" else int((observations["value_mwh"] < 0).sum())
    return NormalizedSeries(
        observations=observations.sort_values("hour_start_utc", ignore_index=True),
        rows_received=len(rows),
        identical_duplicates=len(rows) - len(observations),
        negative_generation_hours=negative_hours,
    )


def normalize_subregional_demand(
    utc_date: date, rows: list[dict[str, object]], *, respondent: str, subregions: tuple[str, ...],
) -> pd.DataFrame:
    """Validate a UTC day of nonnegative demand for each selected subregion."""
    context = f"{utc_date} {respondent} subregional demand"
    if any(not isinstance(row, dict) for row in rows):
        raise ValidationError(f"{context}: invalid records")
    frame = pd.DataFrame(
        rows, columns=["parent", "subba", "value-units", "period", "value"], dtype=object
    )
    if not (frame["parent"].eq(respondent) & frame["subba"].isin(subregions)).all():
        raise ValidationError(f"{context}: mismatched authority or subregion")
    hour_starts, expected, values = _hourly_values(frame, utc_date, context)
    if (values < 0).any():
        raise ValidationError(f"{context}: negative demand values")
    observations = pd.DataFrame({
        "respondent": respondent, "hour_start_utc": hour_starts,
        "subregion": frame["subba"], "value_mwh": values,
    }).drop_duplicates()
    keys = ["hour_start_utc", "subregion"]
    if observations.duplicated(keys).any():
        raise ValidationError(f"{context}: conflicting duplicate values")
    counts = observations.groupby("subregion").size().reindex(subregions, fill_value=0)
    if not counts.eq(len(expected)).all():
        raise ValidationError(f"{context}: missing hours for {', '.join(counts[counts != len(expected)].index)}")
    return observations.sort_values(keys, ignore_index=True)
