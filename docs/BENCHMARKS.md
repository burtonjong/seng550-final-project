# Collect results for the IEEE paper

Complete the [local setup](../README.md) and run the steps below from the repository root. The benchmark runner creates `renewflow_benchmark` in the local PostgreSQL instance and prepares a fresh schema for each trial. Use the existing `.env` and the same Docker Compose stack throughout.

Capture historical responses from the EIA API before timing. Both runners process the same saved responses, so source revisions and EIA request latency stay outside the timed comparison. The results measure ETL execution and orchestration on historical EIA data.

**Experiment 1** measures daily Airflow backfills, task concurrency, and controlled recovery. Start with the capture below. **Experiment 2** measures how throughput and elapsed time change as each of four tasks processes more real EIA observations. [Four-authority benchmark](#experiment-2-four-authority-batches)

## Capture historical EIA snapshots

With `EIA_API_KEY` in `.env`, capture 30 consecutive days from January 15 through February 13, 2025. The end date is exclusive. For a seven-day backfill rehearsal, use `--end-date 2025-01-22`.

```sh
uv run --locked --env-file .env python -m experiments.capture \
  --start-date 2025-01-15 --end-date 2025-02-14 --output-dir .renewflow/eia-replay
```

The capture command reuses existing daily files and fetches any remaining days from EIA. Check that `.renewflow/eia-replay/manifest.json` contains all 30 dates before measuring. If the API connection drops, rerun the command to resume. If the manifest lists an excluded day, choose a complete contiguous range and use those dates consistently for capture and trials.

## Prepare Airflow

Set `RENEWFLOW_REPLAY_DIR=./.renewflow/eia-replay` in `.env` and apply the mount:

```sh
docker compose --profile airflow up -d --wait
docker compose --profile airflow exec -T airflow-scheduler \
  test -r /opt/airflow/replay/manifest.json
```

The second command confirms Airflow can read the manifest. Open the Airflow UI, unpause `renewflow_daily`, and let any active run finish.

## Collect matched measurements

Use the same captured files throughout the trials. Keep other workloads idle and start the trials away from the 19:00 UTC daily schedule. The command creates a fresh results directory. Run the recovery and report commands in the same terminal so `$results_dir` stays available.

Keep the host awake for the timing and recovery trials. On macOS, keep the lid open and replace `uv run` in those commands with `caffeinate -i uv run` to prevent idle sleep. Airflow workers use short-lived tokens for the execution API, including when reading task outputs from XCom. [Airflow worker token lifetime](https://airflow.apache.org/docs/apache-airflow/3.3.2/security/jwt_token_authentication.html#default-timings-execution-api)

```sh
results_dir=$(mktemp -d .renewflow/results-XXXXXX)
printf 'Results directory: %s\n' "$results_dir"
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner suite --start-date 2025-01-15 --end-date 2025-02-14 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir "$results_dir"
```

With 30 complete days, expect 45 measured trials. The suite starts with a one-day workload and advances to 7 and 30 days. It rotates sequential Python, Airflow with one task slot, and Airflow with four task slots, with five repetitions per configuration and workload. A failed verification stops the suite. Keep the failed trial JSON and inspect it before resuming.

The suite also supports 365- and 730-day workloads with three repetitions per configuration. It records unavailable tiers in `suite-coverage.json` when coverage is too short or the shorter trials project a runtime beyond `--long-tier-budget` seconds. Each trial saves timings, task attempts, counts, and database checksums.

### Resume an interrupted suite

On the same host and Compose stack, use the same dates and manifest. Replace `PREVIOUS_RUN_FOLDER` with the directory from the interrupted run. The suite checks each saved result against the current inputs and runtime settings. It copies verified trials to a fresh folder, recalculates Airflow retry counts from the relevant attempts, and runs the missing repetitions. Airflow timings shorter than the server's recorded span by more than one second are excluded. The original trial records and the reasons for exclusions are available in the source folder and `resume.json`, respectively.

```sh
results_dir=$(mktemp -d .renewflow/results-XXXXXX)
printf 'Results directory: %s\n' "$results_dir"
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner suite --start-date 2025-01-15 --end-date 2025-02-14 \
  --manifest .renewflow/eia-replay/manifest.json \
  --resume-from .renewflow/PREVIOUS_RUN_FOLDER --output-dir "$results_dir"
```

If this run stops after copying the verified trials, use its results directory as `--resume-from` on the next attempt. On macOS, prefix the resume command with `caffeinate -i`. Use the completed directory for recovery and report generation.

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

## Run a single trial for troubleshooting

Use either command to check one runner independently after capturing the day. The examples keep troubleshooting outputs in `.renewflow/single-trial`.

### Sequential Python

```sh
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner python --start-date 2025-01-15 --end-date 2025-01-16 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir .renewflow/single-trial
```

### Airflow

Prepare Airflow as above and use `--slots 1` or `--slots 4`.

```sh
uv run --locked --env-file .env python -m experiments.benchmark \
  --runner airflow --slots 4 --start-date 2025-01-15 --end-date 2025-01-16 \
  --manifest .renewflow/eia-replay/manifest.json --output-dir .renewflow/single-trial
```

## Experiment 2 four-authority batches

Use this experiment to test how the Python–Airflow performance gap changes as each task handles more data. In every run, sequential Python processes four authority batches in series. The `renewflow_authority_batches` Airflow DAG runs the same four batches with one or four task slots. Each batch validates and loads all three metrics for one authority using the shared daily PostgreSQL transaction. The number of tasks stays fixed.

| UTC days per authority | Exclusive UTC end | Observations per task | Total observations |
| --- | --- | --- | --- |
| 7 | 2025-01-22 | 504 | 2,016 |
| 30 | 2025-02-14 | 2,160 | 8,640 |
| 90 | 2025-04-15 | 6,480 | 25,920 |

### 1. Capture and verify EIA coverage

Set `EIA_API_KEY` in `.env` and run from the repository root:

```sh
uv run --locked --env-file .env python -m experiments.capture_batch \
  --output-dir .renewflow/batch-replay
```

The capture starts on January 15, 2025, and fetches from EIA directly. It selects CISO and the first three other qualifying authorities from ERCO, MISO, PJM, NYIS, ISNE, and SWPP in that order. It uses the same four in every cohort. If four qualify for 30 days only, the command selects the 7- and 30-day cohorts. If four including CISO lack 30 complete days, it saves the coverage and exits before benchmarking.

If a connection drops during capture, rerun the command to reuse finished authorities. Once the top-level manifest exists, it represents a frozen selection. Use a new directory if you need another capture. Save the manifest, authority submanifests, and snapshots with the results. They record capture provenance, checksums, and exclusions.

### 2. Mount the capture and start Airflow

Set `RENEWFLOW_REPLAY_DIR=./.renewflow/batch-replay` in the same `.env` and rebuild and start the stack:

```sh
docker compose --profile airflow up -d --build --wait
docker compose --profile airflow exec -T airflow-dag-processor airflow dags list-import-errors
```

In the Airflow UI, unpause `renewflow_authority_batches` and let any active runs finish. Keep the machine awake and the stack idle during timing. Run away from the daily 19:00 UTC schedule.

### 3. Measure sequential Python and Airflow

```sh
batch_results=$(mktemp -d .renewflow/batch-results-XXXXXX)
printf 'Batch results directory: %s\n' "$batch_results"
uv run --locked --env-file .env python -m experiments.batch_benchmark \
  --manifest .renewflow/batch-replay/manifest.json --output-dir "$batch_results"
```

On macOS, keep the lid open and prefix the benchmark command with `caffeinate -i`. The suite first runs three 30-day verification trials, one for sequential Python and one for each Airflow slot count. Those records go under `pilot/` and are excluded from the performance report. It then rotates all three configurations for three repetitions each per cohort. Expect 27 measured records if all three cohorts qualify, or 18 if 90 days are unavailable.

Each measured run starts on a fresh PostgreSQL schema. Timing starts at Python process launch or Airflow API submission and ends when the run finishes. Capture, database setup, scheduler reconfiguration, task-instance queries, and checksum verification happen outside the timing interval. Keep any failed trial JSON for diagnosis. If verification fails, resolve the problem and start the batch suite in a fresh directory. The daily backfill suite has its own resume command.

### 4. Generate the batch tables and figure

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report --results-dir "$batch_results"
```

The report checks input and output equality again and plots wall time and throughput against observations per authority task on log scales. Use `--tables-only` without the Matplotlib option to generate CSVs. In the batch reliability table, `days_published` counts authority-days, so a complete 30-day trial publishes 120 authority-days. Use the daily experiment for the controlled recovery comparison. In the batch experiment, compare one-slot and four-slot Airflow to measure the effect of concurrency, then compare both to Python at each workload. If Airflow remains slower, report that outcome for the workloads tested.

After the batch experiment, set `RENEWFLOW_REPLAY_DIR` back to the daily capture path if you need to run Experiment 1 again. Apply the slot count and replay path from `.env` with `docker compose --profile airflow up -d --wait`.
