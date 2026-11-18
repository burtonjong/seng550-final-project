# SENG 550 final project - Renewflow

This project collects hourly solar generation, wind generation, and electricity demand for the California ISO from the U.S. Energy Information Administration (EIA).

The local demo loads a real EIA capture into PostgreSQL. The sequential runner can also fetch and load live historical dates. SQL analytics and Airflow scheduling are later milestones in the [implementation plan](docs/IMPLEMENTATION_PLAN.md).

## Set up the project

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run these commands from the repository root. The first setup needs network access to download Python and dependencies.

```sh
uv python install 3.12
uv sync --locked
```

`uv python install` makes Python 3.12 available. `uv sync --locked` creates `.venv` and installs the project and test dependencies from `uv.lock`. Run project commands through `uv run` to use that environment.

## Run the local PostgreSQL demo

Start Docker with Compose available. Python runs on your machine through `uv`. PostgreSQL 18 runs in a container with a named volume and a port bound to `127.0.0.1`.

Create `.env` from `.env.example` if you need it, then set `POSTGRES_PASSWORD` to a local password. If you already have a `.env` with your EIA key, add the PostgreSQL settings to that file. `POSTGRES_DB` and `POSTGRES_USER` default to `renewflow`. If port `5432` is busy, set `POSTGRES_PORT=5433` in `.env` so both Compose and Python use the new local port.

```sh
if [ ! -f .env ]; then
  (umask 077 && cp .env.example .env)
fi
```

Start PostgreSQL and apply the schema. `--wait` waits for the database health check. The schema command can be rerun on the same database.

```sh
docker compose up -d --wait postgres
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/schema.sql
```

Load the saved day. The runner validates all three series, then writes them in one transaction. An EIA key is optional for this fixture-based demo.

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --snapshot tests/fixtures/eia-ciso-2025-01-15.json
```

On an empty table the command reports 72 inserted or updated observations. Run it again to see 72 validated observations and **0 inserted or updated**. Query PostgreSQL to verify that the stored day still has 24 rows per metric.

```sh
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT metric, count(*) AS observations
FROM hourly_observations
WHERE respondent = 'CISO'
  AND hour_start_utc >= TIMESTAMPTZ '2025-01-15 00:00:00+00'
  AND hour_start_utc < TIMESTAMPTZ '2025-01-16 00:00:00+00'
GROUP BY metric
ORDER BY metric;
SQL
```

`docker compose stop postgres` stops the service and keeps the database volume for the next run.

## Run a live backfill

With PostgreSQL running and `EIA_API_KEY` set in `.env`, pass a UTC date range. The start date is inclusive and the end date is exclusive, so this example loads January 15 only.

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --start-date 2025-01-15 --end-date 2025-01-16
```

The runner commits each complete day separately. If EIA returns incomplete data or the database write fails, the runner exits with an error and keeps the days it has already committed. Repeating the date range can apply revised values to rows with the same key.

## Preview validation offline

After setup, replay the [captured EIA data](tests/fixtures/eia-ciso-2025-01-15.json). This exercises the Pandas validator with real observations and needs neither an EIA key nor a running database. `--offline` tells uv to use the installed and cached dependencies.

```sh
uv run --locked --offline python - <<'PY'
import json
from datetime import date
from pathlib import Path

from renewflow.transform import normalize_series

path = Path("tests/fixtures/eia-ciso-2025-01-15.json")
capture = json.loads(path.read_text(encoding="utf-8"))
day = date.fromisoformat(capture["utc_date"])
total = 0

for metric, series in capture["series"].items():
    result = normalize_series(metric, day, series["response"]["data"])
    hours = result.observations["hour_start_utc"]
    total += len(hours)
    print(f"{metric}: {len(hours)} hours from {hours.iloc[0]} through {hours.iloc[-1]}")
    print(f"  duplicates collapsed: {result.identical_duplicates}")
    print(f"  negative generation hours: {result.negative_generation_hours}")

print(f"Total validated observations: {total}")
PY
```

Expect **72 observations**, with 24 per series from `00:00` through `23:00` UTC. EIA labels each reading by the hour ending, so the following midnight becomes `23:00` on the operating day. The fixture includes **14 negative solar readings**, which the validator preserves and counts.

## Run the tests

```sh
uv run --locked pytest
```

This runs the extraction, validation, and runner tests, and skips database integration tests when the test connection is unset. The tests use a real capture and simulate API failures and multi-day requests. After `uv sync --locked`, you can also use `uv run --locked --offline pytest`.

Create a separate test database once while the Compose service is running.

```sh
docker compose exec -T postgres sh -c 'createdb -U "$POSTGRES_USER" renewflow_test'
```

Add `RENEWFLOW_TEST_DATABASE_URL` to `.env` with the connection URL for `renewflow_test`, for example `postgresql://renewflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/renewflow_test`. Use your actual port and credentials, and percent-encode special characters in the password if needed. The tests create and remove isolated schemas in the test database. They check idempotent reruns, revisions, and rollback after a failed write.

```sh
uv run --locked --env-file .env pytest -m postgres
```

## Run a live EIA check

This optional check needs a network connection to `api.eia.gov` and an [EIA API key](https://www.eia.gov/opendata/register.php). Set `EIA_API_KEY` in your ignored `.env` file.

```sh
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15
```

`--env-file .env` loads the key, and `--date` selects the UTC operating day. The check requests all three series and reports counts, first and last EIA timestamps, and units. For this date, each series should show 24 hour endings from `2025-01-15T01` through `2025-01-16T00` and units of megawatthours.

The final lines confirm the total number of observations and tell you whether the output was a terminal summary or a saved JSON file. To save a fresh sanitized capture, use `--snapshot` with a new path. `mktemp -d` creates a temporary directory so the command can be repeated without replacing an existing fixture.

```sh
snapshot_dir=$(mktemp -d)
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15 \
  --snapshot "$snapshot_dir/eia-ciso-2025-01-15.json"
```

## Project references

- [Project specification](docs/PROJECT_SPEC.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)
- [Airflow pipeline tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html)
