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

| Complete UTC days | Observations |
| ----------------: | -----------: |
|                 1 |        1,152 |
|                 7 |        8,064 |
|                30 |       34,560 |
|               334 |      384,768 |
|               700 |      806,400 |

The 2024–2025 benchmark uses CISO, ERCO, MISO, and PJM and 36 selected load zones. Of 731 calendar days, 700 have shared complete coverage. The 334-day control uses the first 334 eligible dates in the same manifest. Each cohort scans all eight original CSVs once per authority. The logical scan size stays constant while the selected rows grow. Input verification and prior trials can warm filesystem caches.

## 1. Prepare the coverage manifest

Place the January–June and July–December Balance and Subregion CSVs for both 2024 and 2025 in `data/`.

```sh
uv run --locked python -m experiments.import_eia930 \
  --input-dir data --output-dir .renewflow/csv-benchmark \
  --start-date 2024-01-01 --end-date 2026-01-01 --extra-cohort 334
```

This writes only `manifest.json`, with source hashes, ordered dates, zone codes, and coverage exclusions. The same command reuses a matching manifest. Use a new output directory if the files, dates, or cohorts change.

## 2. Start the Compose stack

Use `RENEWFLOW_CSV_DIR=./data` and `RENEWFLOW_REPLAY_DIR=./.renewflow` in `.env`. Airflow reads the benchmark manifest through the read-only mount.

```sh
mkdir -p .renewflow/staging
docker compose --profile airflow up -d --build --wait
docker compose --profile airflow exec -T airflow-dag-processor airflow dags list-import-errors
docker compose --profile airflow exec -T airflow-scheduler \
  sh -c 'test -r /opt/airflow/replay/csv-benchmark/manifest.json && test -x /opt/airflow/staging'
```

Unpause `renewflow_authority_batches` and let other runs finish. If Airflow reports a manifest mismatch, reapply Compose after changing the replay mount. If the container cannot read the public CSVs, check Docker file sharing and their permissions. On macOS, prepend `caffeinate -i` to the benchmark command and leave the lid open.

## 3. Run the benchmark

```sh
results_dir=$(mktemp -d .renewflow/benchmark-results-XXXXXX)
printf 'Results directory: %s\n' "$results_dir"
uv run --locked --env-file .env python -m experiments.batch_benchmark --output-dir "$results_dir"
```

The full suite runs three 30-day pilots, nine clean trials per eligible cohort, and nine seven-day recovery trials. The default five cohorts produce 45 measured clean trials. The pilot JSON goes under `pilot/`. Clean configurations rotate across sequential Python, Airflow with one slot, and Airflow with four slots. The harness configures the slots before timing.

For separate runs, append `--mode clean` to collect the pilots and timings first. Then rerun the command with `--mode recovery` using the same results directory and CSV benchmark manifest. Recovery checks the saved seven-day clean outputs before running. If interrupted during recovery, rerun with `--mode recovery` to reuse verified recovery trials. If a clean trial fails verification, retain its JSON, fix the issue, and start the clean suite in a fresh results directory. On success, the JSON for each Airflow trial includes its run ID.

### Run fault injection against the saved clean trials

Use the default 2024–2025 manifest and the retained results directory. The command uses the existing seven-day clean results to verify recovery and appends nine fault trials. It can resume completed recovery trials in the same directory.

```sh
uv run --locked --env-file .env python -m experiments.batch_benchmark \
  --output-dir .renewflow/extended-results-XXXXX --mode recovery
uv run --locked python -m experiments.report \
  --results-dir .renewflow/extended-results-XXXXX --tables-only
```

Open `recovery.csv` for recovery time, repeated work, and operator intervention. Keep the clean timing table for the scalability comparison.

### What the recovery trials test

CISO’s load fails deliberately just after the third UTC day commits. Both runners automatically retry the load once with a five-second configured delay. They keep the transformed files and revisit the first three dates with idempotent writes. The retry finishes writing the remaining days without duplicate rows. The other authorities and CISO’s upstream stages each run once.

In Airflow’s **Graph** view, open a clean run and verify the four three-stage chains. In **Grid**, inspect a four-slot run for overlapping task times. Open a recovery run from the trial JSON, select `load_authority_0`, and show its first attempt failing and second attempt succeeding. The other 11 stages should succeed on attempt one. Use the final PostgreSQL counts and checksums as publication evidence.

## 4. Generate tables and figures

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report --results-dir "$results_dir"
```

Use `--tables-only` without the Matplotlib option to generate CSVs. Keep the raw JSON and CSV benchmark manifests with the report.

| Output                            | What it shows                                                                                                                                                   |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `timings.csv`, `performance.pdf`  | Median wall time and committed rows per second by workload, with observed minimum and maximum. Includes distinct input file bytes and total logical scan bytes. |
| `comparison.csv`                  | Four-slot speedup over one slot and over Python, plus the measured Airflow–Python time gap. A speedup greater than 1 favors four-slot Airflow.                  |
| `task_timings.csv`                | Stage start, finish, duration, selected row counts, and logical CSV bytes scanned by authority and attempt.                                                     |
| `reliability.csv`, `recovery.csv` | First-attempt outcomes, retry counts, time from the injected failure to CISO load completion, and repeated days and rows.                                       |

Python wall time starts at subprocess launch. Airflow wall time starts at API submission and includes queuing, scheduling, and polling for completion. CSV coverage preparation, schema setup, and checksum verification fall outside timing. CSV reading, parsing, filtering, and staging are included. Collect these trials in a fresh results directory under the direct CSV protocol. Earlier snapshot-based trials used a different extraction workload. Task overlap measures time with at least two stages executing. Launch-to-first-stage delay includes startup and scheduling. All trials use the local PostgreSQL service.

See the [recorded evidence](BENCHMARK_EVIDENCE.md) for the completed 2024–2025 timing run. Report whether four-slot Airflow overtakes Python at a measured workload. If it stays slower, report how the gap changes through the largest verified tier. Task imbalance and a shared database can limit concurrency gains.

Keep the source files, manifest, raw trial JSON, and reports. After verification, completed trial staging can be removed when no tasks are running and no saved DAG run needs those intermediate files. Reports regenerate from the trial JSON.
