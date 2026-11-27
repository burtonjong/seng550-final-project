# RenewFlow implementation plan

[Project specification](PROJECT_SPEC.md) · [Architecture](ARCHITECTURE.md)

## Phases and acceptance checks

| Phase | Deliverable | Acceptance check |
| --- | --- | --- |
| 1. Local input | EIA-930 CSV preparation, application replay, and benchmark source provenance | Import the four 2025 files and verify the 1-, 7-, 30-, and 334-day cohorts. Add files for a new period and verify original snapshots stay readable. Report missing days and reject conflicting overlaps. |
| 2. Transform | Shared normalization for authority-wide series and load-zone demand | Check UTC midnight, missing hours, nonfinite values, leading zero zone IDs, signed generation, and duplicate handling. |
| 3. Storage and sequential replay | PostgreSQL schema, shared transaction, and resumable application loader | Load a range, rerun it, extend it, revise an existing value, and verify rollback restores the prior data and checkpoint. Retain the existing database volume. |
| 4. SQL analytics | Hourly and daily energy summaries, zone demand, and seasonal export | Verify known sums, UTC groups, peak demand, ratio of totals, and counts of contributing days. |
| 5. Airflow and recovery | Manual CISO replay and a four-authority staged benchmark DAG | Import both DAGs. Run a manual day and a staged benchmark under the scheduler. Verify database equivalence and one controlled CISO load retry. |
| 6. Experiments and evidence | Raw results, tables, figures, and local demo | Retain each trial’s provenance and show output equality across all runners before interpreting timings. |

The four source files and frozen cohorts have been verified locally. The PostgreSQL checkpoint suite and Airflow execution under the scheduler still require the local stack. Use the [test guide](TESTING.md) and [benchmark guide](BENCHMARKS.md) to verify them before collecting results. Earlier daily benchmark evidence remains available in the [archive](DAILY_BENCHMARK_ARCHIVE.md).

## Experiment protocol

1. Check coverage of the fixed 2025 CSV set before timing. Freeze the selected UTC dates, files, hashes, and zone codes in a metadata-only manifest.
2. Use four authority chains with three tasks each. Both runners scan the original CSVs in Extract, write staged raw and normalized data, and commit one authority-day at a time.
3. Run the three 30-day pilots, then three clean repetitions per cohort for sequential Python and Airflow with one and four slots. Rotate configuration order and verify row counts and checksums in each fresh trial schema.
4. Collect three seven-day recovery trials per configuration. Inject a CISO load failure after the third selected day commits and verify a successful retry, three repeated days, and matching SQL output. Both runners use a five-second retry delay.
5. Compare median and range for wall time and throughput, and report task overlap, phase durations, logical CSV scan bytes, first-attempt success, and recovery time. Treat a timing crossover as a hypothesis and report the measured outcome. [Commands and outputs](BENCHMARKS.md)

## Remaining work

- Rehearse the local CISO replay and analytics export. Run the PostgreSQL integration tests on the same Compose version used for the demo. [Local demo](LOCAL_DEMO.md) · [Tests](TESTING.md)
- Verify the staged DAG under the scheduler. Collect clean and recovery trials in a fresh results directory, then generate the figures and tables. [Benchmark guide](BENCHMARKS.md)
- Keep the exact input set, manifest hashes, raw trial JSON, figures, tool versions, and contribution notes with the report evidence.
- If additional years are added, make a new replay and review its coverage before changing the benchmark. The 334-day result spans part of January through December 2025 and represents selected complete days.
- Grafana Cloud and EC2 remain optional after local delivery. A cloud visualization can expose the SQL views through a scoped read-only role.

## Submission readiness

The demo ZIP is due December 1, 2026, at 11:59 PM via D2L. The December 2 in-person demonstration is from 8:00 to 9:50 AM in ENA 305. Include source, setup and verification instructions, and the supporting files needed for the demo. Use the approved title and group number in `Group Number SENG550 ProjectTitle Demo`.

Keep measured outputs and source citations for the December 4 report. Codex assisted with repository inspection, planning, the CSV migration, resumable loads, ETL orchestration, benchmark tooling, documentation, and tests. Each contributor should disclose the specific assistance they used and independently verify the work they present.
