# Benchmark evidence

The 2024–2025 timing comparison is saved locally in `.renewflow/extended-results-CEhPVJ/`. It contains 45 verified clean trials and three pilots covering 1, 7, 30, 334, and 700 complete UTC days.

At 334 days, four-slot Airflow took 26.82 seconds and sequential Python took 24.42 seconds. At 700 days and 806,400 observations, Airflow took 31.89 seconds and Python took 41.69 seconds. These are medians across three clean trials per configuration. Both cohorts scanned the same eight CSV files.

The local archive `.renewflow/benchmark-evidence.zip` contains the trial JSON, tables, figure, and manifests for this run. The default `csv-benchmark/manifest.json` matches the original run manifest byte for byte. Keep the eight original CSVs with the evidence when transferring it. This was a clean timing run. To collect fault-injection measurements using the same inputs, follow the [recovery workflow](BENCHMARKS.md#run-fault-injection-against-the-saved-clean-trials).

Regenerate the tables and figure from the saved trials:

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report \
  --results-dir .renewflow/extended-results-CEhPVJ
```

[EIA CSV provenance and historical source research](archive/DATA_SOURCE_RESEARCH.md)
