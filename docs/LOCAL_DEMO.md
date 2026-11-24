# Local demo and analytics

## Prepare the observation table

Start with the [local setup](../README.md). Run these commands from the repository root. Applying the schema again is safe.

```sh
docker compose up -d --wait postgres
docker compose exec -T postgres sh -c \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/schema.sql
```

## Fetch and load a day from EIA

Set `EIA_API_KEY` in `.env`. Choose either runner below. Both fetch one historical day directly from EIA, validate all three series, and write them to the regular `renewflow` database in one transaction.

### Sequential Python

The start date is inclusive and the end date is exclusive. This command fetches January 15, 2025.

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --start-date 2025-01-15 --end-date 2025-01-16
```

### Airflow

If you added `EIA_API_KEY` after starting Airflow, apply the change with `docker compose --profile airflow up -d --wait`. Open `renewflow_daily` in the Airflow UI, unpause it, and choose **Trigger → Single Run**. In **Advanced Options → Configuration JSON**, paste this and trigger the run. Leave **Data Interval** on **Auto**. [Airflow trigger details](BACKFILLS.md#trigger-a-one-day-replay-manually)

```json
{
  "observation_date": "2025-01-15"
}
```

In the Airflow Grid, confirm all three extraction tasks and `load_day` succeed.

## Verify the stored observations

Expect 72 validated observations. A first load inserts 72 rows, and a repeat updates only values EIA has revised. Query PostgreSQL to verify 24 rows per metric for the day.

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

For paper measurements, capture historical data from EIA and use the same capture in both runners. [Benchmark guide](BENCHMARKS.md)
