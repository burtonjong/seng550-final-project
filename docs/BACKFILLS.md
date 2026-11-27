# Historical CSV replay

This guide prepares JSON snapshots for application loads and the one-day demo. The [benchmark](BENCHMARKS.md) uses a metadata-only manifest and reads the original CSVs during Extract.

Run from the repository root after [local setup](../README.md). The CSV importer expects EIA-930 Balance and Subregion files under `data/`. Keep the original files when adding another period and choose a new output directory for each input set.

```sh
uv run --locked python -m experiments.import_eia930 \
  --input-dir data --output-dir .renewflow/csv-replay-2025 --end-date 2026-01-01
```

The default start is January 15, 2025. Supply `--start-date` for earlier history. The end is exclusive and defaults to the day after the last selected observation. Dates excluded during import are recorded by authority in `coverage`. Identical overlaps collapse. For a revision, use a source directory containing the replacement files and the other inputs you still want to include. Conflicting overlaps stop the import and identify the source row.

## Load or resume a range with sequential Python

Apply both schemas once, including on an existing volume, using the [local demo setup](LOCAL_DEMO.md#prepare-postgresql). This command loads January 15 through January 21. Omit both date arguments to load every available authority-day.

```sh
uv run --locked --env-file .env python -m renewflow.cli \
  --manifest .renewflow/csv-replay-2025/manifest.json \
  --start-date 2025-01-15 --end-date 2025-01-22
```

Each authority-day is validated and committed independently. The summary counts committed, updated, skipped, and rejected authority-days. Dates excluded during import appear in the manifest's coverage report. A missing listed snapshot or a failed validation exits unsuccessfully. Other valid days can be committed during the same run.

After interruption, repeat the command. PostgreSQL skips matching completed days and writes changed or uncommitted days. For new periods, add the files, import to a fresh output directory, and pass its manifest to the loader. The loader revalidates selected snapshots on each invocation. An updated manifest can change previously stored values through the same primary keys.

## Trigger a one-day replay manually

Set `RENEWFLOW_REPLAY_DIR=./.renewflow/csv-replay-2025` in `.env` and restart the Airflow profile to apply the mount.

```sh
docker compose --profile airflow up -d --build --wait
```

Open **renewflow_daily**, unpause it, and select **Trigger → Single Run**. Set **Configuration JSON** to the following and leave **Data Interval** on **Auto**.

```json
{
  "observation_date": "2025-01-15",
  "replay_manifest": "CISO/manifest.json"
}
```

`observation_date` selects a completed UTC day of EIA observations. The Airflow Data Interval field does not select the dataset. The manifest path is relative to the mounted replay directory and tells the DAG which local snapshot to open. The three CISO authority-wide series publish to the application database together. For a retry demonstration, add `"fail_first": "2025-01-15:wind"` to the JSON. The first wind attempt fails deliberately, retries, and allows the load to proceed after it succeeds.

Use the [staged benchmark harness](BENCHMARKS.md) for historical Airflow ranges and paper measurements. It submits one `renewflow_authority_batches` DAG run per trial and selects the same manifest dates as the Python runner.
