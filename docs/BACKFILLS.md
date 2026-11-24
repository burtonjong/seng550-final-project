# Backfills and scheduled ingestion

Run commands from the repository root. Start the [local stack](../README.md) and [apply the observation schema](LOCAL_DEMO.md#prepare-the-observation-table) before loading the regular database.

## Run a live backfill with sequential Python

With PostgreSQL running and `EIA_API_KEY` set in `.env`, pass a UTC date range. The start date is inclusive and the end date is exclusive, so this example loads January 15 only.

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --start-date 2025-01-15 --end-date 2025-01-16
```

The runner commits each complete day separately. If EIA returns incomplete data or the database write fails, the runner exits with an error and keeps the days it has already committed. Repeating the date range can apply revised values to rows with the same key.

## Replay a date range with sequential Python

Capture the dates you want to replay, then pass the manifest and UTC range. This example replays January 15 through 21. [Capture commands](BENCHMARKS.md#capture-historical-eia-snapshots).

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --replay-manifest .renewflow/eia-replay/manifest.json \
  --start-date 2025-01-15 --end-date 2025-01-22
```

## Enable the daily schedule

Set `EIA_API_KEY` in `.env` for live ingestion. If you add it after startup, rerun `docker compose --profile airflow up -d --wait` to apply the environment change. Keep PostgreSQL and the Airflow services running, and unpause `renewflow_daily` in the UI.

The DAG runs daily at **19:00 UTC** (`0 19 * * *`) for the **previous completed UTC day**. For example, the scheduled run on January 16 at 19:00 UTC processes January 15 from `00:00` through `23:00` UTC. In the Grid, open the scheduled run and check `utc_date` in the task's `renewflow_event` log to confirm the observation date. Automatic catchup is disabled. To fill a missed range, use the manual backfill below.

## Choose a manual Airflow run

Both procedures use `renewflow_daily`. Use the Airflow web UI's Grid to monitor runs and inspect task attempts.

| Goal | How to submit it |
| --- | --- |
| Check a known day or demonstrate a retry | Use **Single Run** in the web UI and supply an explicit UTC `observation_date`. [One-day replay](#trigger-a-one-day-replay-manually) |
| Fill or replay a range of historical days | Use `airflow backfill create` to preview the logical dates and submit one run per day. [Date-range backfill](#trigger-a-date-range-backfill-manually-in-airflow) |

## Trigger a one-day replay manually

For a manual **Single Run**, set `observation_date` in **Configuration JSON** to select one completed UTC day. For `2025-01-15`, RenewFlow loads the 24 hourly observations per series from January 15 at `00:00 UTC` up to January 16 at `00:00 UTC`, exclusive. Leave **Data Interval** on **Auto** and **Logical Date** at its default. Scheduled runs and native date-range backfills derive the observation date from the end of Airflow’s data interval.

1. In the Airflow web UI, open `renewflow_daily`, unpause it, and click **Trigger** to open the dialog.
2. Select **Single Run**, expand **Advanced Options**, and paste the JSON for your chosen data source below into **Configuration JSON**. [Airflow 3.3.2 run configuration](https://github.com/apache/airflow/blob/3.3.2/airflow-core/docs/core-concepts/dag-run.rst#passing-parameters-when-triggering-dags)
3. Click **Trigger** in the dialog.

### Replay saved EIA data

`replay_manifest` names a file that maps UTC dates to previously fetched EIA snapshots. Airflow looks for the manifest under `RENEWFLOW_REPLAY_DIR`. Use `RENEWFLOW_REPLAY_DIR=./tests/fixtures` in `.env` for the bundled snapshot. If you changed the setting, run `docker compose --profile airflow up -d --wait` to refresh the mount.

```json
{
  "observation_date": "2025-01-15",
  "replay_manifest": "replay-manifest.json"
}
```

In the Grid, open the manually triggered run and confirm that `extract_solar`, `extract_wind`, and `extract_demand` succeed before `load_day`. Confirm 72 observations for January 15. An identical rerun reports zero changed rows. [Row-count query](LOCAL_DEMO.md#verify-the-stored-observations)

### Fetch a day directly from EIA

With only `observation_date` in the JSON, RenewFlow queries EIA for that day and loads the results into local PostgreSQL. Set `EIA_API_KEY` in `.env`. If you added the key after startup, run `docker compose --profile airflow up -d --wait` to apply it.

```json
{
  "observation_date": "2025-01-15"
}
```

### Demonstrate an automatic retry

Trigger a new **Single Run** the same way and use this JSON. The `fail_first` setting causes the first wind attempt to fail. Airflow then retries wind automatically and keeps the successful solar and demand results.

```json
{
  "observation_date": "2025-01-15",
  "replay_manifest": "replay-manifest.json",
  "fail_first": "2025-01-15:wind"
}
```

In the Grid, open `extract_wind` and inspect the logs for both attempts. Expect the first attempt to fail, the second to succeed, and the day's 72 observations to be present after `load_day` succeeds.

To show the same controlled fault on the sequential baseline, use `--fail-first`:

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --replay-manifest tests/fixtures/replay-manifest.json --fail-first 2025-01-15:wind
```

## Trigger a date-range backfill manually in Airflow

For this seven-day example, capture the rehearsal range and set `RENEWFLOW_REPLAY_DIR=./.renewflow/eia-replay` in `.env`. The manifest must list all seven dates. The sequential CLI can replay the manifest in date order or select a subrange with `--start-date` and `--end-date`. [Capture commands](BENCHMARKS.md#capture-historical-eia-snapshots)

Airflow's backfill CLI takes **inclusive logical-date bounds**. Each day's logical date is that date at `19:00 UTC`, so the command below covers January 15 through 21. The equivalent Python and benchmark range uses `--start-date 2025-01-15 --end-date 2025-01-22`, with an exclusive end date. First check that the Airflow dry run lists all seven logical dates. [Airflow 3.3.2 backfills](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/backfill.html)

```sh
docker compose --profile airflow exec -T airflow-scheduler \
  airflow backfill create --dag-id renewflow_daily \
  --from-date 2025-01-15T19:00:00+00:00 --to-date 2025-01-21T19:00:00+00:00 \
  --max-active-runs 1 --reprocess-behavior completed --dry-run \
  --dag-run-conf '{"replay_manifest":"manifest.json"}'
```

After verifying the dates and manifest paths, repeat the command with `--dry-run` removed to submit it. Look for seven daily runs in the Grid. For a live EIA backfill, set the API key and omit the `--dag-run-conf` line and the preceding line's trailing backslash. Use the [benchmark runner](BENCHMARKS.md) for paper measurements.

To keep PostgreSQL running for the Python baseline, stop the Airflow services with `docker compose --profile airflow stop airflow-scheduler airflow-dag-processor airflow-api`. To stop the full stack while retaining its volumes, run `docker compose --profile airflow stop`.
