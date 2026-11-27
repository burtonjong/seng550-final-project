"""Client for the local Airflow API."""

import os
from time import perf_counter
from urllib.parse import urlsplit

import requests


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
