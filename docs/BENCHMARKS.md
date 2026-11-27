# Benchmark sequential Python and Airflow

Complete the [local setup](../README.md) and run the commands from the repository root. RenewFlow’s active experiment asks how Airflow’s execution time, scalability, task reliability, and recovery compare with sequential Python on the same staged ETL workload.

Each trial runs four independent authority chains. Python completes one chain at a time. Airflow can overlap stages across chains using one or four task slots.

```text
Authority 0   Extract → Validate and transform → Load
Authority 1   Extract → Validate and transform → Load
Authority 2   Extract → Validate and transform → Load
Authority 3   Extract → Validate and transform → Load
```

Each Extract task reads the original EIA-930 CSVs, filters for its authority and the selected UTC days, and writes raw records to trial staging. Transform validates and normalizes the three authority-wide series and selected load zones. Load commits one complete authority-day at a time. Both runners call the same stage functions, use the same file format, and start each trial with a fresh benchmark schema. Airflow passes the staging references using XCom.

| UTC days per authority | Calendar end, exclusive | Authority-wide observations | Total with 36 selected load zones |
| --- | --- | ---: | ---: |
| 1 | 2025-01-16 | 288 | 1,152 |
| 7 | 2025-01-22 | 2,016 | 8,064 |
| 30 | 2025-02-14 | 8,640 | 34,560 |
| 334 | 2026-01-01 | 96,192 | 384,768 |

These counts use the four 2025 EIA-930 CSV files for CISO, ERCO, MISO, and PJM. The 334-day cohort spans 351 calendar days starting January 15 and omits 17 dates without shared complete coverage. ERCO’s COAS and FWES are excluded from the fixed zone set because their gaps affect the first 30 days. The manifest records the exact selected dates, per-authority exclusions, source hashes, and cohort counts. The task count stays fixed at 12.

## 1. Prepare the 2025 coverage manifest

Place both Balance and both Subregion files for 2025 under `data/`, then run:

```sh
uv run --locked python -m experiments.import_eia930 \
  --input-dir data --output-dir .renewflow/csv-benchmark-2025 --end-date 2026-01-01 --manifest-only
```

This produces only `manifest.json`. Keep the four CSVs and that manifest with the results so another person can run the same experiment. The command reuses an existing manifest when the source files and date range match. Choose a new output directory when those inputs change. The manifest freezes the selected zone codes, ordered eligible dates, and original file hashes. Each tier takes the first N eligible dates in UTC order.

Coverage selection and file hash verification occur before timing. During every trial, each authority Extract task scans all four original files once. Both runners therefore scan the same logical byte count. Bigger cohorts produce more selected records for transformation and loading, even though the scan size stays constant. Source verification and prior trials can warm filesystem caches. Report the experiment as a local, potentially warm-cache comparison.

### Extend the benchmark with 2024 data

Add both 2024 Balance and Subregion half-years to `data/`. Generate a new manifest with an explicit 334-day control:

```sh
uv run --locked python -m experiments.import_eia930 \
  --input-dir data --output-dir .renewflow/csv-benchmark-2024-2025-control \
  --start-date 2024-01-01 --end-date 2026-01-01 --manifest-only --extra-cohort 334
```

The eight checked files yield 700 shared complete UTC days out of 731 calendar days. They produce 384,768 observations for the 334-day control and 806,400 for the full cohort. The control uses the first 334 eligible dates in the combined manifest. Both cohorts scan the same eight CSVs. See `excluded_utc_dates` and `coverage` in the manifest for gaps and the first validation issue recorded for each excluded authority-day.

Update `RENEWFLOW_REPLAY_DIR` in `.env` to `./.renewflow/csv-benchmark-2024-2025-control` and use the matching `--manifest` path in step 3. Use a new results directory and `--mode clean` for the extended timing comparison.

## 2. Start the Compose stack

Set `RENEWFLOW_CSV_DIR=./data` and `RENEWFLOW_REPLAY_DIR=./.renewflow/csv-benchmark-2025` in the existing `.env`. Airflow mounts both directories read-only. If the container cannot read the public CSVs, ensure they are readable with `chmod a+r data/*.csv` and that Docker can access the project directory. Both runners use `.renewflow/staging` by default. To change it, set `RENEWFLOW_STAGING_DIR` in the same `.env` and create that host directory. The harness makes each individual trial directory writable for the Airflow worker. Use a local staging directory for the public EIA records and stage telemetry.

```sh
mkdir -p .renewflow/staging
docker compose --profile airflow up -d --build --wait
docker compose --profile airflow exec -T airflow-dag-processor airflow dags list-import-errors
docker compose --profile airflow exec -T airflow-scheduler \
  sh -c 'test -r /opt/airflow/replay/manifest.json && test -x /opt/airflow/staging'
```

Unpause `renewflow_authority_batches` in the Airflow UI. Pause competing DAGs for the duration of the experiment after any active runs finish. Keep the host awake. On macOS, prepend `caffeinate -i` to the benchmark command and leave the lid open.

## 3. Run the benchmark

```sh
results_dir=$(mktemp -d .renewflow/staged-results-XXXXXX)
printf 'Results directory: %s\n' "$results_dir"
uv run --locked --env-file .env python -m experiments.batch_benchmark \
  --manifest .renewflow/csv-benchmark-2025/manifest.json --output-dir "$results_dir"
```

The full suite runs three 30-day pilots, nine clean trials per eligible cohort, and nine seven-day recovery trials. There are 36 measured clean trials if all four tiers qualify. The pilot JSON goes under `pilot/`. Clean configurations rotate across sequential Python, Airflow with one slot, and Airflow with four slots. The harness configures the slots before timing.

For separate runs, append `--mode clean` to collect the pilots and timings first. Then rerun the command with `--mode recovery` using the same results directory and CSV benchmark manifest. Recovery checks the saved seven-day clean outputs before running. If interrupted during recovery, rerun with `--mode recovery` to reuse verified recovery trials. If a clean trial fails verification, retain its JSON, fix the issue, and start the clean suite in a fresh results directory. On success, the JSON for each Airflow trial includes its run ID.

### What the recovery trials test

CISO’s load fails deliberately just after the third UTC day commits. Both runners automatically retry the load once with a five-second configured delay. They keep the transformed files and revisit the first three dates with idempotent writes. The retry finishes writing the remaining days without duplicate rows. The other authorities and CISO’s upstream stages each run once.

In Airflow’s **Graph** view, open a clean run and verify the four three-stage chains. In **Grid**, inspect a four-slot run for overlapping task times. Open a recovery run from the trial JSON, select `load_authority_0`, and show its first attempt failing and second attempt succeeding. The other 11 stages should succeed on attempt one. Use the final PostgreSQL counts and checksums as publication evidence.

## 4. Generate tables and figures

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report --results-dir "$results_dir"
```

Use `--tables-only` without the Matplotlib option to generate CSVs. Keep the raw JSON and CSV benchmark manifests with the report.

| Output | What it shows |
| --- | --- |
| `timings.csv`, `performance.pdf` | Median wall time and committed rows per second by workload, with observed minimum and maximum. Includes distinct input file bytes and total logical scan bytes. |
| `comparison.csv` | Four-slot speedup over one slot and over Python, plus the measured Airflow–Python time gap. A speedup greater than 1 favors four-slot Airflow. |
| `task_timings.csv` | Stage start, finish, duration, selected row counts, and logical CSV bytes scanned by authority and attempt. |
| `reliability.csv`, `recovery.csv` | First-attempt outcomes, retry counts, time from the injected failure to CISO load completion, and repeated days and rows. |

Python wall time starts at subprocess launch. Airflow wall time starts at API submission and includes queuing, scheduling, and polling for completion. CSV coverage preparation, schema setup, and checksum verification fall outside timing. CSV reading, parsing, filtering, and staging are included. Collect these trials in a fresh results directory under the direct CSV protocol. Earlier snapshot-based trials used a different extraction workload. Task overlap measures time with at least two stages executing. Launch-to-first-stage delay includes startup and scheduling. All trials use the local PostgreSQL service.

Report whether four-slot Airflow overtakes Python at a measured workload. If it stays slower, report how the gap changes through the largest verified tier. Task imbalance and a shared database can limit concurrency gains. Earlier CISO results and their reproduction context are in the [daily benchmark archive](DAILY_BENCHMARK_ARCHIVE.md).
