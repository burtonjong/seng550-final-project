"""Submit native backfills and read their public Airflow run and task records."""

import os
from datetime import UTC, date, datetime, time
from time import perf_counter, sleep
from urllib.parse import quote, urlsplit

import requests

from renewflow.eia import SERIES

DAG = "renewflow_daily"
STAGES = tuple(f"extract_{metric}" for metric in SERIES) + ("load_day",)


def logical_date(day: date) -> str:
    return datetime.combine(day, time(19), tzinfo=UTC).isoformat()


class Airflow:
    def __init__(self, session: requests.Session):
        self.session = session
        self.url = os.environ.get("RENEWFLOW_AIRFLOW_URL") or (
            f"http://127.0.0.1:{os.environ.get('AIRFLOW_UI_PORT') or '8080'}"
        )
        parsed = urlsplit(self.url)
        if parsed.scheme not in ("http", "https") or parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("benchmark Airflow URL must point to the local API server")
        self.expires = 0.0

    def _request(self, method: str, path: str, **kwargs) -> dict:
        try:
            result = self.session.request(
                method, self.url.rstrip("/") + path, timeout=15, allow_redirects=False, **kwargs
            )
        except requests.RequestException:
            raise RuntimeError(f"could not connect to the Airflow API for {path}") from None
        if not 200 <= result.status_code < 300:
            raise RuntimeError(f"Airflow API returned HTTP {result.status_code} for {path}")
        return result.json()

    def call(self, method: str, path: str, **kwargs) -> dict:
        if perf_counter() >= self.expires:
            password = os.environ.get("AIRFLOW_UI_PASSWORD")
            if not password:
                raise ValueError("set AIRFLOW_UI_PASSWORD to authenticate to the local Airflow API")
            token = self._request("POST", "/auth/token", json={"username": "renewflow", "password": password})
            self.session.headers["Authorization"] = f"Bearer {token['access_token']}"
            self.expires = perf_counter() + 180
        return self._request(method, "/api/v2" + path, **kwargs)

    def prepare(self, days: list[date], slots: int, conf: dict) -> dict:
        body = {
            "dag_id": DAG,
            "from_date": logical_date(days[0]),
            "to_date": logical_date(days[-1]),
            "dag_run_conf": conf,
            "max_active_runs": slots,
            "reprocess_behavior": "completed",
        }
        preview = self.call("POST", "/backfills/dry_run", json=body)["backfills"]
        dates = sorted(datetime.fromisoformat(row["logical_date"]) for row in preview if row["logical_date"])
        if dates != [datetime.fromisoformat(logical_date(day)) for day in days]:
            raise RuntimeError("Airflow dry run did not return exactly the requested UTC dates")
        return body

    def submit(self, body: dict, timeout: int) -> int:
        deadline = perf_counter() + timeout
        backfill_id = self.call("POST", "/backfills", json=body)["id"]
        while perf_counter() < deadline:
            if self.call("GET", f"/backfills/{backfill_id}")["completed_at"] is not None:
                return backfill_id
            sleep(0.5)
        raise TimeoutError(f"Airflow backfill {backfill_id} did not complete within {timeout} seconds")

    def runs(self, body: dict, trial_id: str) -> list[dict]:
        offset = 0
        runs = []
        while True:
            response = self.call(
                "GET", f"/dags/{DAG}/dagRuns",
                params={
                    "logical_date_gte": body["from_date"], "logical_date_lte": body["to_date"],
                    "limit": 100, "offset": offset, "order_by": "logical_date",
                },
            )
            batch = response["dag_runs"]
            runs.extend(run for run in batch if (run.get("conf") or {}).get("benchmark_trial_id") == trial_id)
            offset += len(batch)
            if not batch or offset >= response["total_entries"]:
                return runs

    def attempts(self, runs: list[dict]) -> list[dict]:
        records = []
        for run in runs:
            run_id = run["dag_run_id"]
            day = datetime.fromisoformat(run["logical_date"]).astimezone(UTC).date().isoformat()
            path = f"/dags/{DAG}/dagRuns/{quote(run_id, safe='')}/taskInstances"
            for task in self.call("GET", path)["task_instances"]:
                stage = task["task_id"]
                if stage not in STAGES:
                    continue
                tries = [task]
                if task["try_number"] > 1:
                    tries = self.call("GET", f"{path}/{stage}/tries")["task_instances"] + tries
                for number, attempt in sorted({row["try_number"]: row for row in tries}.items()):
                    if attempt["start_date"] and number > 0:
                        records.append({
                            "runner": "airflow", "run_id": run_id, "utc_date": day, "stage": stage,
                            "attempt": number, "status": attempt["state"], "started_at": attempt["start_date"],
                            "finished_at": attempt["end_date"], "duration_seconds": attempt["duration"],
                        })
        return records
