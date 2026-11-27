# Local demo and analytics

## Prepare PostgreSQL

Start with the [local setup](../README.md). Run these commands from the repository root. Apply both files on an existing database to add the checkpoint table and load-zone storage.

```sh
docker compose up -d --wait postgres
cat sql/schema.sql sql/subregional_demand.sql | docker compose exec -T postgres sh -c \
  'psql -X -v ON_ERROR_STOP=1 --single-transaction -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -'
```

## Replay a day

Create a local CSV replay using the [import instructions](BACKFILLS.md). Both examples below publish January 15, 2025, to the application database.

### Sequential Python

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --replay-manifest .renewflow/csv-replay-2025/CISO/manifest.json \
  --start-date 2025-01-15 --end-date 2025-01-16
```

### Airflow

Mount the replay directory, then trigger `renewflow_daily` manually. In the Airflow Grid, confirm all three extraction tasks and `load_day` succeed. [Airflow setup and trigger JSON](BACKFILLS.md#trigger-a-one-day-replay-manually)

## Verify the stored observations

Expect 72 validated observations. An empty database receives 72 rows. A repeat with matching values skips the completed day. Query PostgreSQL to verify 24 rows per metric for the day.

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

Run this from the repository root to save three CSVs in a fresh folder under `exports/`. The shell writes the files directly to your computer.

```sh
(
  set -e
  mkdir -p exports
  export_dir=$(mktemp -d "$PWD/exports/energy-XXXXXX")
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

On macOS, run `open exports` from the repository root to browse the CSVs in Finder.

If January 15 is the only day loaded, expect 24 hourly rows and one daily row. The daily solar-and-wind ratio uses the energy totals for the day. Multiply it by 100 to display it as a percentage of demand.

The seasonal CSV uses complete UTC days and reports how many days contribute to each hourly mean, plus their first and last dates. January 15 contributes one winter day. Load dates across multiple seasons before using the export for a seasonal comparison in the report.

For paper measurements, use the same imported CSV replay in both runners. [Benchmark guide](BENCHMARKS.md)
