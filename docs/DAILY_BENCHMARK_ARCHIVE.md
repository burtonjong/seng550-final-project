# Archived daily backfill benchmark

The earlier CISO experiment used frozen hourly EIA observations for January 15 through February 13, 2025, and the former scheduled daily DAG. It collected 45 verified timing trials across 1, 7, and 30 UTC days, plus two controlled wind-extraction retry trials. Each workload compared sequential Python with one-slot and four-slot Airflow. The current [staged benchmark](BENCHMARKS.md) uses a different task graph and input set.

Keep the original replay files, run JSON, and generated reports together. To reproduce the old execution protocol, use the code revision and runtime configuration associated with the original trial records and its saved replay manifest. To regenerate figures and tables from the saved trials on the current branch, set `results_dir` to the completed archive directory and run:

```sh
uv run --locked --with matplotlib==3.11.2 python -m experiments.report --results-dir "$results_dir"
```

For CSV tables, run `uv run --locked python -m experiments.report --results-dir "$results_dir" --tables-only`.
