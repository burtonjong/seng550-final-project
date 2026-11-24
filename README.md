# RenewFlow

RenewFlow is a SENG 550 project that compares sequential Python and Apache Airflow ETL using hourly solar, wind, and demand data from the U.S. Energy Information Administration (EIA). PostgreSQL and Airflow run locally in Docker Compose.

## Install and configure

Start Docker Desktop or OrbStack, install [uv](https://docs.astral.sh/uv/getting-started/installation/), and run these commands from the repository root.

```sh
uv python install 3.12
uv sync --locked
test -f .env || cp .env.example .env
```

Edit the single `.env` file. Set `POSTGRES_PASSWORD`, `AIRFLOW_DB_PASSWORD`, and `AIRFLOW_UI_PASSWORD`. Generate a value for `AIRFLOW_JWT_SECRET` with `openssl rand -hex 32`. Keep the database passwords consistent when reusing a Docker volume.

PostgreSQL defaults to host port `5432`. If another service already uses it, set `POSTGRES_PORT=5433`. Airflow defaults to `AIRFLOW_UI_PORT=8080`. Set `EIA_API_KEY` in `.env` for the live demo and benchmark data capture.

## Start the local stack

```sh
docker compose --profile airflow up -d --build --wait
```

`airflow-init` is a one-time migration job. It should exit with code `0`. PostgreSQL, the Airflow API, the scheduler, and the DAG processor should keep running.

## Verify startup

```sh
docker compose --profile airflow exec -T airflow-dag-processor airflow dags list-import-errors
```

Confirm there are no DAG import errors. Open `http://localhost:8080` or the port in `AIRFLOW_UI_PORT`. Sign in as `renewflow` using `AIRFLOW_UI_PASSWORD` and confirm you can see `renewflow_daily`. If initialization fails, inspect `docker compose --profile airflow logs --tail=80 airflow-init`.

## What to run next

**To collect IEEE paper results, start by capturing 30 days from EIA.** Then run the matched timing suite, the recovery trials, and the report generator. [Benchmark instructions](docs/BENCHMARKS.md#capture-historical-eia-snapshots)

| Goal | Guide |
| --- | --- |
| Run pytest and PostgreSQL integration checks | [Testing](docs/TESTING.md) |
| Run a specific live or replay backfill, or enable the daily schedule | [Backfills and scheduled ingestion](docs/BACKFILLS.md) |
| Fetch one day live from EIA and export energy analytics | [Local demo and analytics](docs/LOCAL_DEMO.md) |
| Review system design and remaining work | [Project specification](docs/PROJECT_SPEC.md) · [Implementation plan](docs/IMPLEMENTATION_PLAN.md) |
