import json
import os
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

import requests

from renewflow import eia

DAY = date(2025, 1, 15)
TEST_KEY = "test-only-secret"
EXPECTED_SERIES = (
    ("solar", "fuel-type-data", "fueltype", "SUN"),
    ("wind", "fuel-type-data", "fueltype", "WND"),
    ("demand", "region-data", "type", "D"),
)


def response(rows, total=None):
    return Mock(
        status_code=200,
        **{
            "json.return_value": {
                "response": {"total": str(len(rows) if total is None else total), "data": rows},
                "apiVersion": "test-version",
                "request": {"params": {"api_key": TEST_KEY}},
            }
        },
    )


def hourly_rows(facet, code):
    periods = [f"2025-01-15T{hour:02d}" for hour in range(1, 24)] + ["2025-01-16T00"]
    return [
        {"period": period, "respondent": "CISO", facet: code, "value": "10", "value-units": "megawatthours"}
        for period in periods
    ]


class ExtractTests(TestCase):
    def setUp(self):
        self.session = Mock(spec=requests.Session)

    def fetch(self, metric="solar"):
        return eia.fetch_series(self.session, metric, DAY, api_key=TEST_KEY)

    def test_routes_and_midnight_bounds(self):
        for metric, route, facet, code in EXPECTED_SERIES:
            with self.subTest(metric=metric):
                rows = hourly_rows(facet, code)
                self.session.get.return_value = response(rows)
                self.assertEqual(self.fetch(metric).rows, rows)
                self.session.get.assert_called_with(
                    f"https://api.eia.gov/v2/electricity/rto/{route}/data/",
                    params={
                        "api_key": TEST_KEY,
                        "frequency": "hourly",
                        "data[0]": "value",
                        "facets[respondent][]": "CISO",
                        f"facets[{facet}][]": code,
                        "start": "2025-01-15T01",
                        "end": "2025-01-16T00",
                        "sort[0][column]": "period",
                        "sort[0][direction]": "asc",
                        "length": 5000,
                        "offset": 0,
                    },
                    timeout=(5, 30),
                    allow_redirects=False,
                )

    def test_reads_all_pages_using_actual_page_lengths(self):
        rows = hourly_rows("fueltype", "SUN")
        self.session.get.side_effect = [response(rows[:8], 24), response(rows[8:], 24)]
        result = self.fetch()
        self.assertEqual(result.rows, rows)
        self.assertEqual(result.api_version, "test-version")
        self.assertEqual(
            [call.kwargs["params"]["offset"] for call in self.session.get.call_args_list], [0, 8]
        )

    def test_rejects_incomplete_or_inconsistent_pages(self):
        row = hourly_rows("fueltype", "SUN")[0]
        for pages in (
            [response([], 1)],
            [response([row], 2), response([], 2)],
            [response([row], 2), response([row], 3)],
            [response([row, row], 1)],
        ):
            with self.subTest(pages=len(pages)):
                self.session.get.side_effect = pages
                with self.assertRaisesRegex(eia.EIAError, "page sequence"):
                    self.fetch()

    def test_accepts_a_valid_empty_result_for_downstream_validation(self):
        self.session.get.return_value = response([])
        self.assertEqual(self.fetch().rows, [])
        self.session.get.assert_called_once()

    def test_http_errors_are_classified_without_including_response_text(self):
        for status, expected in (
            (401, eia.EIAError), (403, eia.EIAError), (302, eia.EIAError),
            (429, eia.RetryableEIAError), (503, eia.RetryableEIAError),
        ):
            with self.subTest(status=status):
                http_response = Mock(status_code=status, text=TEST_KEY)
                self.session.get.return_value = http_response
                with self.assertRaisesRegex(expected, f"HTTP {status}") as error:
                    self.fetch()
                self.assertIs(type(error.exception), expected)
                self.assertNotIn(TEST_KEY, str(error.exception))
                http_response.json.assert_not_called()

    def test_transport_error_tracebacks_omit_request_details(self):
        for exception in (requests.Timeout, requests.ConnectionError):
            with self.subTest(exception=exception):
                self.session.get.side_effect = exception(f"?api_key={TEST_KEY}")
                try:
                    self.fetch()
                except eia.RetryableEIAError:
                    self.assertNotIn(TEST_KEY, traceback.format_exc())
                else:
                    self.fail("expected a retryable EIA error")

    def test_rejects_malformed_json_without_echoing_the_body(self):
        self.session.get.return_value = Mock(status_code=200)
        self.session.get.return_value.json.side_effect = ValueError(TEST_KEY)
        with self.assertRaisesRegex(eia.EIAError, "malformed JSON"):
            self.fetch()

    def test_rejects_malformed_data_envelopes(self):
        for body in (None, {"total": True}, {"total": "one"}, {"total": 1, "data": [False]}):
            with self.subTest(body=body):
                self.session.get.return_value = Mock(status_code=200)
                self.session.get.return_value.json.return_value = {"response": body}
                with self.assertRaisesRegex(eia.EIAError, "envelope|row total|list of records"):
                    self.fetch()

    def test_requires_a_key_before_requesting_data(self):
        with self.assertRaisesRegex(eia.EIAError, "EIA_API_KEY"):
            eia.fetch_series(self.session, "solar", DAY, api_key="")
        self.session.get.assert_not_called()


class SmokeCommandTests(TestCase):
    def smoke(self, snapshot, pages):
        argv = ["eia", "--date", DAY.isoformat(), "--snapshot", str(snapshot)]
        with (
            patch.dict(os.environ, {"EIA_API_KEY": TEST_KEY}),
            patch("sys.argv", argv),
            patch.object(requests.Session, "get", side_effect=pages),
        ):
            eia.main()

    def test_capture_keeps_provenance_and_can_be_read_without_credentials(self):
        pages = []
        for _, _, facet, code in EXPECTED_SERIES:
            rows = hourly_rows(facet, code)
            rows[0]["request"] = {"api_key": TEST_KEY}
            pages.append(response(rows))
        with TemporaryDirectory() as directory, redirect_stdout(StringIO()) as output:
            snapshot = Path(directory) / "fixtures" / "day.json"
            self.smoke(snapshot, pages)
            content = snapshot.read_text(encoding="utf-8")

        self.assertNotIn(TEST_KEY, content)
        self.assertNotIn('"request"', content)
        capture = json.loads(content)
        self.assertEqual(capture["utc_date"], DAY.isoformat())
        for metric, route, facet, code in EXPECTED_SERIES:
            self.assertIn(f"{metric} | rows=24", output.getvalue())
            series = capture["series"][metric]
            self.assertIn(route, series["route"])
            self.assertEqual(series["facets"], {"respondent": "CISO", facet: code})
            self.assertEqual(series["apiVersion"], "test-version")
            self.assertEqual(series["response"]["data"], hourly_rows(facet, code))

    def test_incomplete_day_does_not_write_a_snapshot(self):
        with (
            TemporaryDirectory() as directory,
            redirect_stdout(StringIO()),
            redirect_stderr(StringIO()) as error,
        ):
            snapshot = Path(directory) / "day.json"
            rows = hourly_rows("fueltype", "SUN")[:-1]
            with self.assertRaises(SystemExit) as exit_status:
                self.smoke(snapshot, [response(rows)])
            self.assertEqual(exit_status.exception.code, 1)
            self.assertFalse(snapshot.exists())
            self.assertIn("24 expected UTC hour endings", error.getvalue())
