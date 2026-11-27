# RenewFlow

RenewFlow is a SENG 550 project that compares sequential Python and Apache Airflow on the same renewable-energy ETL workload. It imports local EIA-930 Balance and Subregion CSVs, validates hourly solar generation, wind generation, and demand, and publishes observations and SQL summaries to PostgreSQL. The application supports manual replay and resumable imports. PostgreSQL and Airflow run locally in Docker Compose.

The benchmark reads the original CSV files during four independent Extract → Transform → Load chains and compares sequential Python with Airflow at one and four task slots. The verified CSV coverage manifest provides 1-, 7-, 30-, and 334-day workloads. Trials measure wall time, throughput, task overlap, and recovery from a controlled load failure. [Architecture](docs/ARCHITECTURE.md) · [Benchmark guide](docs/BENCHMARKS.md)

## Install and configure

Start Docker Desktop or OrbStack, install [uv](https://docs.astral.sh/uv/getting-started/installation/), and run these commands from the repository root.

```sh
uv python install 3.12
uv sync --locked
test -f .env || cp .env.example .env
```

Edit the single `.env` file. Set `POSTGRES_PASSWORD`, `AIRFLOW_DB_PASSWORD`, and `AIRFLOW_UI_PASSWORD`. Generate a value for `AIRFLOW_JWT_SECRET` with `openssl rand -hex 32`. Keep the database passwords consistent when reusing a Docker volume.

PostgreSQL defaults to host port `5432`. If another service already uses it, set `POSTGRES_PORT=5433`. Airflow defaults to `AIRFLOW_UI_PORT=8080`.

## Start the local stack

```sh
mkdir -p .renewflow/staging
docker compose --profile airflow up -d --build --wait
```

`airflow-init` is a one-time migration job. It should exit with code `0`. PostgreSQL, the Airflow API, the scheduler, and the DAG processor should keep running.

## Verify startup

```sh
docker compose --profile airflow exec -T airflow-dag-processor airflow dags list-import-errors
```

Confirm there are no DAG import errors. Open `http://localhost:8080` or the port in `AIRFLOW_UI_PORT`. Sign in as `renewflow` using `AIRFLOW_UI_PASSWORD` and confirm you can see `renewflow_daily` and `renewflow_authority_batches`. If initialization fails, inspect `docker compose --profile airflow logs --tail=80 airflow-init`.

## What to run next

| Goal | Guide |
| --- | --- |
| Collect or regenerate IEEE benchmark results | [Benchmark instructions](docs/BENCHMARKS.md) |
| Run pytest and PostgreSQL integration checks | [Testing](docs/TESTING.md) |
| Import CSV files, resume a PostgreSQL load, or trigger a manual DAG run | [Historical CSV replay](docs/BACKFILLS.md) |
| Replay one day and export energy analytics | [Local demo and analytics](docs/LOCAL_DEMO.md) |
| Review system design and remaining work | [Project specification](docs/PROJECT_SPEC.md) · [Implementation plan](docs/IMPLEMENTATION_PLAN.md) |
| Review earlier CISO daily benchmark results | [Daily benchmark archive](docs/DAILY_BENCHMARK_ARCHIVE.md) |
