"""Initialize local Airflow metadata storage and UI credentials."""

import json
import os
import sys
from pathlib import Path
from urllib.parse import quote


def initialize() -> None:
    required = {
        "AIRFLOW_DB_PASSWORD": "AIRFLOW_DB_PASSWORD",
        "AIRFLOW_UI_PASSWORD": "AIRFLOW_UI_PASSWORD",
        "AIRFLOW__API_AUTH__JWT_SECRET": "AIRFLOW_JWT_SECRET",
    }
    missing = [setting for variable, setting in required.items() if not os.environ.get(variable, "").strip()]
    if missing:
        raise SystemExit(f"Set {', '.join(missing)} in .env before starting Airflow")

    import psycopg
    from psycopg import sql

    from renewflow.db import connect_observations

    if "airflow" in (os.environ.get("POSTGRES_USER"), os.environ.get("POSTGRES_DB")):
        raise SystemExit("Use a separate user and database for RenewFlow observations")
    password = os.environ["AIRFLOW_DB_PASSWORD"]
    try:
        with connect_observations() as connection:
            if connection.execute("SELECT 1 FROM pg_roles WHERE rolname = 'airflow'").fetchone() is None:
                connection.execute(sql.SQL("CREATE ROLE airflow LOGIN PASSWORD {}").format(sql.Literal(password)))
            if connection.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'").fetchone() is None:
                connection.execute("CREATE DATABASE airflow OWNER airflow")
    except psycopg.Error:
        raise SystemExit("Could not initialize the Airflow metadata database") from None

    path = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
    os.umask(0o077)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"renewflow": os.environ["AIRFLOW_UI_PASSWORD"]}), encoding="utf-8")
    temporary.replace(path)


if __name__ == "__main__":
    if sys.argv[1:] == ["metadata-url"]:
        password = quote(os.environ["AIRFLOW_DB_PASSWORD"], safe="")
        print(f"postgresql+psycopg2://airflow:{password}@postgres:5432/airflow")
    else:
        initialize()
