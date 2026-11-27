# RenewFlow implementation plan

[Project specification](PROJECT_SPEC.md) · [Architecture](ARCHITECTURE.md)

## Phases and acceptance checks

| Phase | Deliverable | Acceptance check |
| --- | --- | --- |
| 1. Local input | EIA-930 CSV preparation and benchmark source provenance | Import the eight 2024–2025 files and verify the 1-, 7-, 30-, 334-, and 700-day cohorts. Add files for a new period and verify earlier manifests still identify the same source files. Report missing days and reject conflicting overlaps. |
| 2. Transform | Shared normalization for authority-wide series and load-zone demand | Check UTC midnight, missing hours, nonfinite values, leading zero zone IDs, signed generation, and duplicate handling. |
| 3. Storage and sequential runner | PostgreSQL schema and shared authority-day transaction | Verify idempotent upserts, revised values, and transactional rollback. |
| 4. SQL analytics | Hourly and daily energy summaries, zone demand, and seasonal export | Verify known sums, UTC groups, peak demand, ratio of totals, and counts of contributing days. |
| 5. Airflow and recovery | Four-authority benchmark DAG | Import the DAG and run the staged benchmark under the scheduler. Verify database equivalence and one controlled CISO load retry. |
| 6. Experiments and evidence | Raw results, tables, figures, and local demo | Retain each trial’s provenance and show output equality across all runners before interpreting timings. |

The 2024–2025 timing comparison ran successfully under the local stack. See [benchmark evidence](BENCHMARK_EVIDENCE.md) for the saved inputs and trial records. The PostgreSQL transaction checks are documented separately. [Test guide](TESTING.md)

## Experiment protocol

1. Check coverage of the fixed 2024–2025 CSV set before timing. Freeze the selected UTC dates, files, hashes, and zone codes in a metadata-only manifest.
2. Use four authority chains with three tasks each. Both runners scan the original CSVs in Extract, write staged raw and normalized data, and commit one authority-day at a time.
3. Run the three 30-day pilots, then three clean repetitions per cohort for sequential Python and Airflow with one and four slots. Rotate configuration order and verify row counts and checksums in each fresh trial schema.
4. Collect three seven-day recovery trials per configuration. Inject a CISO load failure after the third selected day commits and verify a successful retry, three repeated days, and matching SQL output. Both runners use a five-second retry delay.
5. Compare median and range for wall time and throughput, and report task overlap, phase durations, logical CSV scan bytes, first-attempt success, and recovery time. Treat a timing crossover as a hypothesis and report the measured outcome. [Commands and outputs](BENCHMARKS.md)

## Remaining work

- Rehearse the benchmark for the presentation and run the PostgreSQL integration tests against the local stack. [Tests](TESTING.md)
- Keep the exact input set, manifest hashes, raw trial JSON, figures, tool versions, and contribution notes with the report evidence.
- Grafana Cloud and EC2 remain optional after local delivery. A cloud visualization can expose the SQL views through a scoped read-only role.

## Submission readiness

The demo ZIP is due December 1, 2026, at 11:59 PM via D2L. The December 2 in-person demonstration is from 8:00 to 9:50 AM in ENA 305. Include source, setup and verification instructions, and the supporting files needed for the demo. Use the approved title and group number in `Group Number SENG550 ProjectTitle Demo`.

Keep measured outputs and source citations for the December 4 report. Codex assisted with repository inspection, planning, the CSV migration, ETL orchestration, benchmark tooling, documentation, and tests. Each contributor should disclose the specific assistance they used and independently verify the work they present.
