"""Regenerate IEEE tables and figures from recorded benchmark trials."""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median


def summarize(trials: list[dict]) -> tuple[list[dict], list[dict]]:
    timings = defaultdict(list)
    outputs = {}
    reliability = []
    experiments = {result.get("experiment") for result in trials if "trial_id" in result}
    if len(experiments) > 1:
        raise ValueError("report each experiment from its own results directory")
    subregional = experiments == {"authority_batches_subregions"}
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
        })
        if result["outcome"] != "success":
            continue
        cohort = (result["start_date"], result["end_date"], result["days"])
        captures = result["input"]["captures"]
        evidence = (
            tuple(sorted((day, capture["sha256"]) for day, capture in captures.items())),
            *(result["output"][key] for key in ("observation_sha256", "hourly_sha256", "daily_sha256")),
            *((result["output"]["subregional_sha256"], result["output"]["daily_subregional_sha256"]) if subregional else ()),
        )
        if outputs.setdefault(cohort, evidence) != evidence:
            raise ValueError(f"snapshots or SQL outputs differ for cohort starting {cohort[0]}")
        if not result["fault"]:
            timings[(*cohort, result["runner"], result["slots"])].append(result)

    summary = []
    for (start, end, days, runner, slots), runs in sorted(timings.items()):
        runtime = [run["wall_seconds"] for run in runs]
        throughput = [run["observations_per_second"] for run in runs]
        summary.append({
            "start_date": start, "end_date": end, "days": days, "runner": runner, "slots": slots,
            "observations": runs[0]["output"]["observations"], "input_bytes": runs[0]["input"]["input_bytes"],
            **({"observations_per_task": runs[0]["observations_per_task"]} if experiments == {"authority_batches"} else {}),
            **({"subregional_observations": runs[0]["output"]["subregional_observations"]} if subregional else {}),
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
        tasks = [
            {
                "trial_id": trial["trial_id"], "runner": trial["runner"], "slots": trial["slots"],
                "days": trial["days"], "authority": event["authority"], "observations": event["expected_observations"],
                "status": event["status"], "duration_seconds": event.get("duration_seconds"),
            }
            for trial in trials if trial.get("experiment") == "authority_batches_subregions"
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
