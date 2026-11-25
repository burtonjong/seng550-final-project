"""Fetch EIA-930 observations for UTC operating days."""

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import requests

API_URL = "https://api.eia.gov/v2"
RESPONDENT = "CISO"
SERIES = {
    "solar": ("electricity/rto/fuel-type-data/data/", "fueltype", "SUN"),
    "wind": ("electricity/rto/fuel-type-data/data/", "fueltype", "WND"),
    "demand": ("electricity/rto/region-data/data/", "type", "D"),
}


class EIAError(Exception):
    """An extraction error that is safe to report in a run log."""


class RetryableEIAError(EIAError):
    """An extraction error for which the caller may retry the task."""


@dataclass
class SeriesExtract:
    rows: list[dict[str, object]]
    api_version: str | None


def _hour_endings(utc_date: date) -> list[str]:
    # The EIA hourly route represents UTC periods without an offset suffix.
    midnight = datetime.combine(utc_date, time.min)
    return [
        (midnight + timedelta(hours=hour)).isoformat(timespec="hours")
        for hour in range(1, 25)
    ]


def _request_page(
    session: requests.Session, route: str, params: dict[str, str | int]
) -> tuple[list[dict[str, object]], int, str | None]:
    try:
        response = session.get(
            f"{API_URL}/{route}",
            params=params,
            timeout=(5, 30),
            allow_redirects=False,
        )
    except requests.Timeout:
        raise RetryableEIAError("EIA request timed out") from None
    except requests.ConnectionError:
        raise RetryableEIAError("could not connect to EIA") from None
    except requests.RequestException:
        raise EIAError("EIA request failed") from None

    status = response.status_code
    if status == 429 or 500 <= status < 600:
        raise RetryableEIAError(f"EIA returned HTTP {status}")
    if status in (401, 403):
        raise EIAError(f"EIA rejected authentication (HTTP {status})")
    if status != 200:
        raise EIAError(f"EIA returned HTTP {status}")

    try:
        payload = response.json()
    except ValueError:
        raise EIAError("EIA returned malformed JSON") from None

    body = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(body, dict):
        raise EIAError("EIA response is missing its data envelope")
    total = body.get("total")
    if isinstance(total, str) and total.isdecimal():
        try:
            total = int(total)
        except ValueError:
            raise EIAError("EIA response has an invalid row total") from None
    if type(total) is not int or total < 0:
        raise EIAError("EIA response has an invalid row total")
    rows = body.get("data")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise EIAError("EIA response data must be a list of records")
    api_version = payload.get("apiVersion")
    if api_version is not None and not isinstance(api_version, str):
        raise EIAError("EIA response has an invalid API version")
    return rows, total, api_version


def fetch_series(
    session: requests.Session, metric: str, utc_date: date, *, api_key: str
) -> SeriesExtract:
    """Fetch one metric. Preserve source values for downstream validation."""
    return fetch_series_range(
        session, metric, RESPONDENT, utc_date, utc_date + timedelta(days=1), api_key=api_key
    )


def fetch_series_range(
    session: requests.Session,
    metric: str,
    respondent: str,
    start: date,
    end: date,
    *,
    api_key: str,
) -> SeriesExtract:
    """Fetch one authority and metric over a UTC range with an exclusive end."""
    if metric not in SERIES:
        raise ValueError("metric must be solar, wind, or demand")
    if start >= end:
        raise ValueError("EIA date range must include at least one UTC day")
    if not api_key.strip():
        raise EIAError("EIA_API_KEY is required")
    route, facet, code = SERIES[metric]
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": respondent,
        f"facets[{facet}][]": code,
        "start": _hour_endings(start)[0],
        "end": f"{end.isoformat()}T00",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 5000,
    }
    rows, total, api_version = _request_page(session, route, {**params, "offset": 0})
    if len(rows) > total or (not rows and total):
        raise EIAError("EIA returned an incomplete or inconsistent page sequence")
    while len(rows) < total:
        page, page_total, _ = _request_page(session, route, {**params, "offset": len(rows)})
        if page_total != total or not page or len(rows) + len(page) > total:
            raise EIAError("EIA returned an incomplete or inconsistent page sequence")
        rows.extend(page)

    fields = ("period", "respondent", facet, "value", "value-units")
    records = [{field: row[field] for field in fields if field in row} for row in rows]
    return SeriesExtract(records, api_version)


def _check_smoke_result(metric: str, result: SeriesExtract, expected: list[str]) -> None:
    _, facet, code = SERIES[metric]
    periods = [row.get("period") for row in result.rows]
    first = periods[0] if periods and periods[0] in expected else "unavailable"
    last = periods[-1] if periods and periods[-1] in expected else "unavailable"
    unit_ok = bool(result.rows) and all(
        row.get("value-units") == "megawatthours" for row in result.rows
    )
    units = "megawatthours" if unit_ok else "unexpected or missing"
    print(f"{metric} | rows={len(periods)} | first={first} | last={last} | units={units}")

    if periods != expected:
        raise EIAError(f"{metric} did not return the 24 expected UTC hour endings")
    if not unit_ok:
        raise EIAError(f"{metric} did not report megawatthours")
    if any(row.get("respondent") != RESPONDENT or row.get(facet) != code for row in result.rows):
        raise EIAError(f"{metric} returned a mismatched series identifier")


def save_snapshot(
    path: Path, day: date, extracts: dict[str, SeriesExtract], *, api_key: str, respondent: str = RESPONDENT
) -> None:
    series = {}
    for metric, (route, facet, code) in SERIES.items():
        result = extracts[metric]
        series[metric] = {
            "route": route,
            "facets": {"respondent": respondent, facet: code},
            "apiVersion": result.api_version,
            "response": {"total": len(result.rows), "data": result.rows},
        }
    capture = {
        "utc_date": day.isoformat(),
        "captured_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "frequency": "hourly",
        "series": series,
    }
    contents = json.dumps(capture, indent=2) + "\n"
    if api_key and api_key in contents:
        raise EIAError("snapshot contains an API key")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        output.write(contents)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="UTC operating date")
    parser.add_argument("--snapshot", type=Path, help="save a sanitized capture to a new JSON file")
    args = parser.parse_args()
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not api_key:
        parser.error("set EIA_API_KEY in the environment or load .env with uv --env-file")

    series = {}
    try:
        expected = _hour_endings(args.date)
        with requests.Session() as session:
            for metric in SERIES:
                try:
                    result = fetch_series(session, metric, args.date, api_key=api_key)
                except EIAError as error:
                    raise EIAError(f"{args.date} {metric}: {error}") from None
                _check_smoke_result(metric, result, expected)
                series[metric] = result
        output_status = "Terminal summary only. Add --snapshot PATH to save a sanitized JSON file."
        if args.snapshot:
            save_snapshot(args.snapshot, args.date, series, api_key=api_key)
            output_status = f"Saved sanitized JSON snapshot to {args.snapshot}"

        total_observations = len(expected) * len(series)
        print(f"Verified {total_observations} {RESPONDENT} observations for {args.date} UTC.")
        print(output_status)
    except (EIAError, OSError, OverflowError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
