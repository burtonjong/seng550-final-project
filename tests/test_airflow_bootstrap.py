import os
import subprocess
import sys
from pathlib import Path


def test_metadata_url_command_emits_exact_connection_url():
    script = Path(__file__).resolve().parents[1] / "docker" / "airflow_bootstrap.py"
    output = subprocess.check_output(
        [sys.executable, str(script), "metadata-url"],
        env=os.environ | {"AIRFLOW_DB_PASSWORD": "test-only:x/y@z"},
    )

    assert output == b"postgresql+psycopg2://airflow:test-only%3Ax%2Fy%40z@postgres:5432/airflow"
