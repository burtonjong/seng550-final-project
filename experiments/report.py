"""Regenerate IEEE tables and figures from recorded benchmark trials."""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median

from experiments import CSV_EXPERIMENT, STAGED_EXPERIMENT, input_identity


def summarize(trials: list[dict]) -> tuple[list[dict], list[dict]]:
    timings = defaultdict(list)
    outputs = {}
    reliability = []
    experiments = {result.get("experiment") for result in trials if "trial_id" in result}
    if len(experiments) > 1:
        raise ValueError("report each experiment from its own results directory")
    staged = experiments in ({STAGED_EXPERIMENT}, {CSV_EXPERIMENT})
    subregional = staged or experiments == {"authority_batches_subregions"}
    for result in trials:
        if "trial_id" not in result:
            continue
        metrics = result.get("metrics", {})
        reliability.append({
            "trial_id": result["trial_id"], "runner": result["runner"], "slots": result["slots"],
            "start_date": result["start_date"], "end_date": result["end_date"], "days": result["days"],
            "fault": result["fault"], "outcome": result["outcome"],
            "first_attempt_successes": metrics.get("first_attempt_successes"),
            "attempted_stages": metrics.get("attempted_stages"), "retries": metrics.get("retries"),
            "stages_without_first_attempt": len(metrics["stages_without_first_attempt"]) if metrics else None,
            "days_published": result.get("output", {}).get("days_published"),
            "recovery_seconds": metrics.get("recovery_seconds"),
            "recovery_repeated_stages": metrics.get("recovery_repeated_stages"),
            "recovery_required_operator": metrics.get("recovery_required_operator"),
            **({
                "recovery_repeated_days": metrics.get("recovery_repeated_days"),
                "recovery_repeated_rows": metrics.get("recovery_repeated_rows"),
            } if staged else {}),
        })
        if result["outcome"] != "success":
            continue
        cohort = (result["start_date"], result["end_date"], result["days"])
        evidence = (
            input_identity(result["input"]),
            *(result["output"][key] for key in ("observation_sha256", "hourly_sha256", "daily_sha256")),
            *((result["output"]["subregional_sha256"], result["output"]["daily_subregional_sha256"]) if subregional else ()),
        )
        if outputs.setdefault(cohort, evidence) != evidence:
            raise ValueError(f"inputs or SQL outputs differ for cohort starting {cohort[0]}")
        if not result["fault"]:
            timings[(*cohort, result["runner"], result["slots"])].append(result)

    summary = []
    for (start, end, days, runner, slots), runs in sorted(timings.items()):
        runtime = [run["wall_seconds"] for run in runs]
        throughput = [run["observations_per_second"] for run in runs]
        summary.append({
            "start_date": start, "end_date": end, "days": days, "runner": runner, "slots": slots,
            "observations": runs[0]["output"]["observations"], "input_bytes": runs[0]["input"]["input_bytes"],
            **({"distinct_csv_bytes": runs[0]["input"]["distinct_csv_bytes"]} if experiments == {CSV_EXPERIMENT} else {}),
            **({"observations_per_task": runs[0]["observations_per_task"]} if experiments == {"authority_batches"} else {}),
            **({"subregional_observations": runs[0]["output"]["subregional_observations"]} if subregional else {}),
            **({
                "first_stage_delay_seconds_median": median(run["metrics"]["first_stage_delay_seconds"] for run in runs),
                "peak_concurrent_stages_median": median(run["metrics"]["peak_concurrent_stages"] for run in runs),
                "stage_overlap_seconds_median": median(run["metrics"]["stage_overlap_seconds"] for run in runs),
            } if staged else {}),
            "trials": len(runs), "wall_seconds_median": median(runtime), "wall_seconds_min": min(runtime),
            "wall_seconds_max": max(runtime), "rows_per_second_median": median(throughput),
            "rows_per_second_min": min(throughput), "rows_per_second_max": max(throughput),
        })
    return summary, reliability


def write_csv(path: Path, rows: list[dict]) -> None:
    if rows:
        with path.open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


def comparisons(summary: list[dict]) -> list[dict]:
    workloads = defaultdict(dict)
    for row in summary:
        workloads[row["start_date"], row["end_date"]][row["runner"], row["slots"]] = row
    rows = []
    for (start, end), configs in sorted(workloads.items()):
        if not {("python", 1), ("airflow", 1), ("airflow", 4)} <= configs.keys():
            continue
        python = configs["python", 1]["wall_seconds_median"]
        airflow_one = configs["airflow", 1]["wall_seconds_median"]
        airflow_four = configs["airflow", 4]["wall_seconds_median"]
        rows.append({
            "start_date": start, "end_date": end, "days": configs["python", 1]["days"],
            "observations": configs["python", 1]["observations"],
            "airflow_four_vs_one_speedup": airflow_one / airflow_four,
            "airflow_four_vs_python_speedup": python / airflow_four,
            "airflow_four_minus_python_seconds": airflow_four - python,
            "airflow_four_faster_than_python": airflow_four < python,
        })
    return rows


def plot(path: Path, summary: list[dict]) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = defaultdict(list)
    for row in summary:
        series[(row["start_date"], row["runner"], row["slots"])].append(row)
    figure, axes = plt.subplots(1, 2, figsize=(9, 3.4), layout="constrained")
    workload = "days"
    if "subregional_observations" in summary[0]:
        workload = "observations"
    elif "observations_per_task" in summary[0]:
        workload = "observations_per_task"
    for (start, runner, slots), rows in series.items():
        points = sorted(rows, key=lambda row: row[workload])
        label = "Sequential Python" if runner == "python" else f"Airflow ({slots} slot{'s' if slots != 1 else ''})"
        if len({row["start_date"] for row in summary}) > 1:
            label += f" · {start}"
        for axis, metric in zip(axes, ("wall_seconds", "rows_per_second"), strict=True):
            values = [point[f"{metric}_median"] for point in points]
            errors = [[abs(point[f"{metric}_{bound}"] - value) for point, value in zip(points, values)]
                      for bound in ("min", "max")]
            axis.errorbar([point[workload] for point in points], values, yerr=errors, marker="o", label=label)
    axes[0].set_ylabel("Wall time (seconds)")
    axes[1].set_ylabel("Throughput (observations/second)")
    for axis in axes:
        axis.set_xscale("log")
        if workload != "days":
            sizes = sorted({row[workload] for row in summary})
            axis.set_xticks(sizes, labels=[f"{size:,}" for size in sizes])
            axis.set_yscale("log")
            axis.set_xlabel("Observations per trial" if workload == "observations" else "Observations per authority task")
        else:
            axis.set_xlabel("Historical workload (UTC days)")
        axis.grid(alpha=0.25)
    axes[0].legend(fontsize=8)
    figure.savefig(path)
    plt.close(figure)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--tables-only", action="store_true", help="generate CSV tables without Matplotlib")
    args = parser.parse_args()
    try:
        trials = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(args.results_dir.glob("*.json"))]
        summary, reliability = summarize(trials)
        if not reliability:
            raise ValueError("no benchmark trials found")
        write_csv(args.results_dir / "reliability.csv", reliability)
        staged = any(trial.get("experiment") in (STAGED_EXPERIMENT, CSV_EXPERIMENT) for trial in trials)
        csv_trial = any(trial.get("experiment") == CSV_EXPERIMENT for trial in trials)
        if staged:
            write_csv(args.results_dir / "recovery.csv", [row for row in reliability if row["fault"]])
            write_csv(args.results_dir / "comparison.csv", comparisons(summary))
        tasks = [
            {
                "trial_id": trial["trial_id"], "runner": trial["runner"], "slots": trial["slots"],
                "days": trial["days"], "authority": event["authority"], "observations": event.get("expected_observations"),
                "status": event["status"], "duration_seconds": event.get("duration_seconds"),
                **({
                    "fault": trial["fault"], "stage": event["stage"], "attempt": event["attempt"],
                    "started_at": event["started_at"], "finished_at": event["finished_at"],
                    "rows_processed": event["rows_processed"], "rows_changed": event.get("rows_changed"),
                    "days_committed": event.get("days_committed"),
                } if staged else {}),
                **({"csv_bytes_scanned": event.get("csv_bytes_scanned")} if csv_trial else {}),
            }
            for trial in trials if trial.get("experiment") in (STAGED_EXPERIMENT, CSV_EXPERIMENT, "authority_batches_subregions")
            for event in trial.get("events", []) if "authority" in event
        ]
        write_csv(args.results_dir / "task_timings.csv", tasks)
        if summary:
            write_csv(args.results_dir / "timings.csv", summary)
            if not args.tables_only:
                plot(args.results_dir / "performance.pdf", summary)
        print(f"Saved report outputs in {args.results_dir}")
    except ModuleNotFoundError:
        parser.exit(1, "Install Matplotlib for the plot or run with --tables-only.\n")
    except (KeyError, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
