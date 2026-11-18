import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from zoneinfo import ZoneInfo

import pandas as pd

from renewflow.transform import ValidationError, normalize_series

DAY = date(2025, 1, 15)
FIXTURE = Path(__file__).parent / "fixtures" / "eia-ciso-2025-01-15.json"


def solar_rows(day=DAY):
    midnight = datetime(day.year, day.month, day.day)
    return [
        {
            "period": (midnight + timedelta(hours=hour)).isoformat(timespec="hours"),
            "respondent": "CISO",
            "fueltype": "SUN",
            "value": str(hour),
            "value-units": "megawatthours",
        }
        for hour in range(1, 25)
    ]


class TransformTests(TestCase):
    def test_real_capture_yields_72_unique_observations(self):
        capture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        results = {
            metric: normalize_series(metric, DAY, series["response"]["data"])
            for metric, series in capture["series"].items()
        }
        frame = pd.concat([result.observations for result in results.values()], ignore_index=True)
        self.assertEqual(list(frame.columns), ["respondent", "hour_start_utc", "metric", "value_mwh"])
        self.assertEqual(len(frame), 72)
        self.assertEqual(frame.groupby("metric").size().to_dict(), {"solar": 24, "wind": 24, "demand": 24})
        self.assertFalse(frame.duplicated(["respondent", "hour_start_utc", "metric"]).any())
        self.assertEqual(frame["hour_start_utc"].min(), datetime(2025, 1, 15, tzinfo=UTC))
        self.assertEqual(frame["hour_start_utc"].max(), datetime(2025, 1, 15, 23, tzinfo=UTC))
        self.assertEqual(results["solar"].negative_generation_hours, 14)
        self.assertEqual(frame.loc[frame["metric"] == "solar", "value_mwh"].min(), Decimal("-56"))
        self.assertTrue(all(isinstance(value, Decimal) for value in frame["value_mwh"]))

    def test_daylight_saving_fall_back_keeps_24_distinct_utc_hours(self):
        day = date(2025, 11, 2)
        hours = normalize_series("solar", day, solar_rows(day)).observations["hour_start_utc"]
        local = hours.dt.tz_convert(ZoneInfo("America/Los_Angeles"))
        self.assertEqual(len(hours), 24)
        self.assertTrue(hours.is_unique)
        self.assertEqual(list(local.dt.strftime("%H %z")[local.dt.hour == 1]), ["01 -0700", "01 -0800"])

    def test_collapses_identical_duplicates_and_preserves_exact_decimals(self):
        rows = solar_rows()
        rows[0]["value"] = "123.456789012345678901"
        rows.append({**rows[0], "value": "123.4567890123456789010"})
        result = normalize_series("solar", DAY, list(reversed(rows)))
        self.assertEqual(result.rows_received, 25)
        self.assertEqual(result.identical_duplicates, 1)
        self.assertEqual(result.observations.iloc[0]["value_mwh"], Decimal(rows[0]["value"]))

    def test_rejects_conflicting_duplicates(self):
        rows = solar_rows()
        rows.append({**rows[0], "value": "999"})
        with self.assertRaisesRegex(ValidationError, "1 hours with conflicting duplicate values"):
            normalize_series("solar", DAY, rows)

    def test_reports_the_missing_interval_including_missing_final_hour(self):
        with self.assertRaisesRegex(ValidationError, "1 missing hours.*2025-01-15T23:00Z"):
            normalize_series("solar", DAY, solar_rows()[:-1])
        with self.assertRaisesRegex(ValidationError, "24 missing hours"):
            normalize_series("demand", DAY, [])

    def test_rejects_out_of_day_and_malformed_timestamps(self):
        for period, message in (
            ("2025-01-15T00", "outside the UTC day"),
            ("2025-01-16T01", "outside the UTC day"),
            ("2025-01-15T01:30", "invalid timestamps"),
            ("2025-02-30T01", "invalid timestamps"),
            ("2025-01-15T01-08:00", "invalid timestamps"),
            (None, "invalid timestamps"),
        ):
            with self.subTest(period=period):
                rows = solar_rows()
                rows[0]["period"] = period
                with self.assertRaisesRegex(ValidationError, message):
                    normalize_series("solar", DAY, rows)

    def test_rejects_missing_and_malformed_values(self):
        for value in (None, "", "bad", True, "NaN", "Infinity", float("-inf")):
            with self.subTest(value=value):
                rows = solar_rows()
                rows[0]["value"] = value
                with self.assertRaisesRegex(ValidationError, "1 missing, nonnumeric, or nonfinite"):
                    normalize_series("solar", DAY, rows)
        rows = solar_rows()
        del rows[0]["value"]
        with self.assertRaisesRegex(ValidationError, "1 missing, nonnumeric, or nonfinite"):
            normalize_series("solar", DAY, rows)

    def test_rejects_mismatched_and_missing_identifiers_or_units(self):
        for field, value, message in (
            ("respondent", "OTHER", "mismatched series identifiers"),
            ("fueltype", "WND", "mismatched series identifiers"),
            ("fueltype", None, "mismatched series identifiers"),
            ("value-units", "megawatts", "invalid units"),
            ("value-units", None, "invalid units"),
        ):
            with self.subTest(field=field, value=value):
                rows = solar_rows()
                rows[0][field] = value
                with self.assertRaisesRegex(ValidationError, message):
                    normalize_series("solar", DAY, rows)

    def test_preserves_signed_generation_and_rejects_nonpositive_demand(self):
        rows = solar_rows()
        rows[0]["value"] = "0"
        rows[1]["value"] = "-2.5"
        result = normalize_series("solar", DAY, rows)
        self.assertEqual(list(result.observations["value_mwh"].head(2)), [Decimal("0"), Decimal("-2.5")])
        self.assertEqual(result.negative_generation_hours, 1)

        demand = [{**row, "type": "D"} for row in rows]
        with self.assertRaisesRegex(ValidationError, "2 nonpositive demand values"):
            normalize_series("demand", DAY, demand)
