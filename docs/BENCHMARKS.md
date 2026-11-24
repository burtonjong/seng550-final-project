# Collect results for the IEEE paper

Complete the [local setup](../README.md) and run the steps below from the repository root. The benchmark runner creates `renewflow_benchmark` in the local PostgreSQL instance and prepares a fresh schema for each trial. Use the existing `.env` and the same Docker Compose stack throughout.

Capture historical responses from the EIA API before timing. Both runners process the same saved responses, so source revisions and EIA request latency stay outside the timed comparison. The results measure ETL execution and orchestration on historical EIA data.

## Run the one-day pilot

Set `EIA_API_KEY` in `.env` and capture the first day directly from EIA:

```sh
uv run --locked --env-file .env python -m experiments.capture \
  --start-date 2025-01-15 --end-date 2025-01-16 --output-dir .renewflow/eia-replay
```

### Sequential Python

```sh
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner python --start-date 2025-01-15 --end-date 2025-01-16 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir .renewflow/pilot
```

### Airflow

Set `RENEWFLOW_REPLAY_DIR=./.renewflow/eia-replay` in `.env` and run `docker compose --profile airflow up -d --wait` to mount the capture. Open the Airflow UI, unpause `renewflow_daily`, and let any active run finish.

```sh
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner airflow --slots 4 --start-date 2025-01-15 --end-date 2025-01-16 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir .renewflow/pilot
```

Both trials should report `success` and publish the same 72 observations. Inspect the trial JSON in `.renewflow/pilot` for the observation and analytics checksums. Keep the pilot separate from the paper results.

## Capture historical EIA snapshots

With `EIA_API_KEY` in `.env`, capture 30 consecutive days from January 15 through February 13, 2025. The end date is exclusive. For a seven-day backfill rehearsal, use `--end-date 2025-01-22`.

```sh
uv run --locked --env-file .env python -m experiments.capture \
  --start-date 2025-01-15 --end-date 2025-02-14 --output-dir .renewflow/eia-replay
```

The capture command reuses the pilot's saved day and fetches the rest from EIA. Check that `.renewflow/eia-replay/manifest.json` contains all 30 dates before measuring. If the API connection drops, rerun the command to resume. If the manifest lists an excluded day, choose a complete contiguous range and use those dates consistently for capture and trials.

If you changed `RENEWFLOW_REPLAY_DIR` to `./.renewflow/eia-replay` in `.env`, apply the new mount:

```sh
docker compose --profile airflow up -d --wait
```

## Collect matched measurements

Use the same captured files throughout the trials. Keep other workloads idle and start the trials away from the 19:00 UTC daily schedule. The command creates a fresh results directory. Run the recovery and report commands in the same terminal so `$results_dir` stays available.

```sh
results_dir=$(mktemp -d .renewflow/results-XXXXXX)
printf 'Results directory: %s\n' "$results_dir"
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner suite --start-date 2025-01-15 --end-date 2025-02-14 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir "$results_dir"
```

With 30 complete days, expect 45 trials across 1-, 7-, and 30-day workloads. The suite rotates sequential Python, Airflow with one task slot, and Airflow with four task slots, with five repetitions per configuration and workload. It also supports 365- and 730-day workloads with three repetitions per configuration. It records unavailable tiers in `suite-coverage.json` when coverage is too short or the pilot projects a long-tier runtime beyond `--long-tier-budget` seconds. Each trial saves timings, task attempts, counts, and database checksums. If a trial fails verification, keep its JSON and inspect it before starting a fresh set of trials.

For a single trial over a different range, use the sequential Python or Airflow command from the pilot with your dates and a manifest that covers them. Use `--slots 1` or `--slots 4` for the Airflow trial.

## Measure recovery from a transient failure

Run one controlled wind extraction failure for each runner. The trial records retry attempts, time to publication, and output checksums.

### Sequential Python recovery

```sh
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner python --start-date 2025-01-15 --end-date 2025-01-16 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir "$results_dir" \
  --fail-first 2025-01-15:wind
```

### Airflow recovery

```sh
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner airflow --slots 4 --start-date 2025-01-15 --end-date 2025-01-16 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir "$results_dir" \
  --fail-first 2025-01-15:wind
```

After the Airflow trials, apply the scheduler slot count from `.env` with `docker compose --profile airflow up -d --wait airflow-scheduler`.

## Generate tables and figures

Generate `timings.csv`, `reliability.csv`, and `performance.pdf` from the recorded trials.

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report --results-dir "$results_dir"
```

Use `uv run --locked python -m experiments.report --results-dir "$results_dir" --tables-only` to generate just the CSV files. The report checks snapshot and output equality across each cohort. Keep the raw trial JSON, manifest, tables, figure, and coverage notes for the IEEE paper.
