# SENG 550 final project - Renewflow

This project collects hourly solar generation, wind generation, and electricity demand for the California ISO from the U.S. Energy Information Administration (EIA).

The local demo loads a real EIA capture into PostgreSQL and exports hourly, daily, and seasonal analytics. Run the pipeline from the sequential CLI or the Airflow DAG. See the [implementation plan](docs/IMPLEMENTATION_PLAN.md) for progress and verification status.

## Set up the project

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run these commands from the repository root. The first setup needs network access to download Python and dependencies.

```sh
uv python install 3.12
uv sync --locked
```

`uv python install` makes Python 3.12 available. `uv sync --locked` creates `.venv` and installs the project and test dependencies from `uv.lock`. Run project commands through `uv run` to use that environment.

## Run the local PostgreSQL demo

Start Docker with Compose available. Python runs on your machine through `uv`. The default Compose startup runs PostgreSQL 18 with a named volume and a port bound to `127.0.0.1`. Enable the `airflow` profile when you want to run the DAG.

Create `.env` from `.env.example` if you need it, then set `POSTGRES_PASSWORD` to a local password. If you already have a `.env` with your EIA key, add the PostgreSQL settings to that file. `POSTGRES_DB` and `POSTGRES_USER` default to `renewflow`

Start PostgreSQL and apply the schema. `--wait` waits for the database health check. The schema command can be rerun on the same database.

```sh
docker compose up -d --wait
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

## Export energy analytics

With local PostgreSQL running and observations loaded, apply the views. This command can be rerun as the views change.

```sh
docker compose exec -T postgres sh -c \
  'psql -X -v ON_ERROR_STOP=1 --single-transaction -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -' \
  < sql/analytics.sql
```

Export the loaded data to three CSVs in a fresh local directory.

```sh
(
  set -e
  export_dir=$(mktemp -d)
  docker compose exec -T -e PGTZ=UTC postgres sh -c \
    'psql -X --csv -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
      -c "SELECT * FROM hourly_energy_summary ORDER BY respondent, hour_start_utc"' \
    > "$export_dir/hourly.csv"
  docker compose exec -T postgres sh -c \
    'psql -X --csv -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
      -c "SELECT * FROM daily_energy_summary ORDER BY respondent, utc_date"' \
    > "$export_dir/daily.csv"
  docker compose exec -T postgres sh -c \
    'psql -X --csv -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < sql/seasonal_profile.sql > "$export_dir/seasonal_profile.csv"
  printf 'CSV exports saved in %s\n' "$export_dir"
)
```

With the January 15 fixture alone, expect 24 hourly rows and one daily row. The daily row should show 589,463 MWh of demand and a peak hourly demand of 27,956 MWh. The daily solar-and-wind ratio uses the energy totals for the day. Multiply a ratio by 100 to display it as a percentage of demand.

The seasonal CSV uses complete UTC days and reports how many days contribute to each hourly mean, plus their first and last dates. The fixture contributes one winter day. Load dates across multiple seasons before using the export for a seasonal comparison in the report.

## Run a live backfill

With PostgreSQL running and `EIA_API_KEY` set in `.env`, pass a UTC date range. The start date is inclusive and the end date is exclusive, so this example loads January 15 only.

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --start-date 2025-01-15 --end-date 2025-01-16
```

The runner commits each complete day separately. If EIA returns incomplete data or the database write fails, the runner exits with an error and keeps the days it has already committed. Repeating the date range can apply revised values to rows with the same key.

When you're done with the PostgreSQL-only demo, `docker compose stop postgres` stops the service and keeps its database volume.

## Run Airflow locally

Start with the [local PostgreSQL setup](#run-the-local-postgresql-demo) through the schema command. Set `AIRFLOW_DB_PASSWORD`, `AIRFLOW_UI_PASSWORD`, and `AIRFLOW_JWT_SECRET` in the ignored `.env`. Generate the JWT secret with `openssl rand -hex 32`. Keep `AIRFLOW_DB_PASSWORD` consistent across starts with the same PostgreSQL volume.

```sh
docker compose --profile airflow up -d --build --wait
docker compose --profile airflow exec -T airflow-dag-processor \
  airflow dags list-import-errors
```

The `airflow` profile starts PostgreSQL and Airflow 3.3.2 with `LocalExecutor`, the API server, and the DAG processor. The initialization job creates the PostgreSQL metadata database and role if needed. Open `http://localhost:8080` and sign in as `renewflow` using `AIRFLOW_UI_PASSWORD`. If that port is busy, set `AIRFLOW_UI_PORT` in `.env` before starting the stack.

### Replay one day in the DAG

Unpause `renewflow_daily` in the UI, then trigger a replay from the terminal:

```sh
docker compose --profile airflow exec -T airflow-scheduler \
  airflow dags trigger renewflow_daily \
  --conf '{"observation_date":"2025-01-15","replay_manifest":"replay-manifest.json"}'
```

The DAG reads the included manifest and real EIA fixture from the read-only replay mount. In the Grid view, confirm that `extract_solar`, `extract_wind`, and `extract_demand` succeed before `load_day`. An empty observation table gets 72 rows. If you already loaded the fixture with the sequential CLI, the load reports zero changed rows. Both runners log `renewflow_event` records with stage, attempt, duration, and row counts. The replay needs only local PostgreSQL. Automatic runs use live EIA and require `EIA_API_KEY`.

For a controlled retry, add `"fail_first":"2025-01-15:wind"` to the run configuration. The first wind attempt fails, and that task retries while Airflow retains successful solar and demand work. Confirm the failed and successful attempts in the wind task log and check the final row counts. The sequential comparison accepts the same fault target through `--fail-first`:

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --replay-manifest tests/fixtures/replay-manifest.json --fail-first 2025-01-15:wind
```

### Backfill a UTC range in Airflow

For a multi-day replay, put the sanitized daily snapshots and a manifest under `RENEWFLOW_REPLAY_DIR` before starting the stack. Follow the [sample manifest](tests/fixtures/replay-manifest.json) and list every requested UTC date with its relative snapshot filename. The sequential CLI can replay the whole manifest in order or select a subrange with `--start-date` and `--end-date`.

At 19:00 UTC on January 16, the scheduled DAG processes January 15. In Airflow's backfill CLI, the logical date is the interval start, so the logical date for January 15 observations is January 15 at 19:00 UTC. For January 15 through 17, first check that the dry run lists the three logical dates:

```sh
docker compose --profile airflow exec -T airflow-scheduler \
  airflow backfill create --dag-id renewflow_daily \
  --from-date 2025-01-15T19:00:00+00:00 --to-date 2025-01-17T19:00:00+00:00 \
  --max-active-runs 1 --reprocess-behavior completed --dry-run \
  --dag-run-conf '{"replay_manifest":"manifest.json"}'
```

After verifying the dates and the manifest paths, repeat the command with `--dry-run` removed. Omit `--dag-run-conf` for a live EIA backfill with the API key set. For benchmarks, use an isolated observations database and record the exact manifest, Airflow task slots and active-run limit, and initial database state.

To keep PostgreSQL running for the Python baseline, stop the Airflow services with `docker compose --profile airflow stop airflow-scheduler airflow-dag-processor airflow-api`. To stop the full stack while retaining its volumes, run `docker compose --profile airflow stop`.

## Rehearse the demo and run the paper benchmarks

Capture seven days before the demo. The end date is exclusive. The capture command resumes from existing daily files and records source metadata, checksums, bytes, row counts, and excluded dates in `manifest.json`.

```sh
mkdir -p .renewflow/replay
cp -n tests/fixtures/eia-ciso-2025-01-15.json .renewflow/replay/
uv run --locked --env-file .env python -m experiments.capture \
  --start-date 2025-01-15 --end-date 2025-01-22 --output-dir .renewflow/replay
```

Set `RENEWFLOW_REPLAY_DIR=./.renewflow/replay` in `.env` and start Airflow. Trigger the January 15 replay with `"replay_manifest":"manifest.json"` in its configuration. Repeat with `"fail_first":"2025-01-15:wind"`, show the wind retry in the Grid, and use the [row-count query](#run-the-local-postgresql-demo) to verify 24 observations per series. Rehearse the seven-day native backfill by setting its last logical-date bound to `2025-01-21T19:00:00+00:00`.

### Collect matched measurements

Create `.env.benchmark` with local credentials based on `.env.example`. Set the following values to use separate ports, a dedicated database, and the captured input directory.

```dotenv
POSTGRES_DB=renewflow_benchmark
POSTGRES_PORT=5433
AIRFLOW_UI_PORT=8081
RENEWFLOW_REPLAY_DIR=./.renewflow/replay
EIA_API_KEY=
```

```sh
docker compose --env-file .env.benchmark -p renewflow-benchmark --profile airflow up -d --build --wait
```

Open `http://localhost:8081` and unpause `renewflow_daily`. Let any initial scheduled run reach a terminal state and keep the benchmark stack clear of other runs during timing. Capture a larger contiguous historical range for longer cohorts and keep those files fixed during trials. The example uses the same seven days as the rehearsal.

```sh
uv run --locked --env-file .env.benchmark python -m experiments.benchmark \
  --runner suite --start-date 2025-01-15 --end-date 2025-01-22 \
  --manifest .renewflow/replay/manifest.json --output-dir .renewflow/results
```

The suite rotates sequential Python, Airflow with one task slot, and Airflow with four task slots across nested cohorts of 1, 7, 30, 365, and 730 days. It runs five trials per configuration for pilot tiers and three for longer tiers. It records unavailable tiers in `suite-coverage.json` when coverage is too short or the pilot projects a long-tier runtime beyond `--long-tier-budget` seconds. Each trial gets an empty PostgreSQL schema and saves timings, task attempts, counts, and database checksums. Airflow runs a dry run before timing and is polled every half second through native backfill completion. For one trial, use `--runner python` or `--runner airflow`. Use `--slots 1` or `--slots 4` to select Airflow concurrency.

Collect transient recovery evidence separately for both runners.

```sh
for runner in python airflow; do
  uv run --locked --env-file .env.benchmark python -m experiments.benchmark \
    --runner "$runner" --slots 4 --start-date 2025-01-15 --end-date 2025-01-16 \
    --manifest .renewflow/replay/manifest.json --output-dir .renewflow/results \
    --fail-first 2025-01-15:wind
done
```

Generate tables and the Matplotlib PDF from the recorded trials.

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report --results-dir .renewflow/results
```

Use `uv run --locked python -m experiments.report --results-dir .renewflow/results --tables-only` for `timings.csv` and `reliability.csv`. The report checks snapshot and output equality across each cohort. Keep the raw trial JSON, manifest, tables, figure, and coverage notes for the IEEE paper.


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

Add `RENEWFLOW_TEST_DATABASE_URL` to `.env` with the connection URL for `renewflow_test`, for example `postgresql://renewflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/renewflow_test`. Use your actual port and credentials, and percent-encode special characters in the password if needed. The tests create and remove isolated schemas in the test database. They check idempotent reruns, revisions, rollback, and SQL analytics. Hosted PostgreSQL deployment is deferred until near the end of the project. A running local test database is sufficient for the pending integration checks.

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
