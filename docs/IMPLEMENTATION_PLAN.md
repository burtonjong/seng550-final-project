# RenewFlow implementation plan

## Current repository and proposed layout

The project directory was empty at initial inspection. Create the paths below as their phases need them.

```text
renewflow/
├── AGENTS.md
├── README.md                  # Setup, commands, and demo walkthrough
├── docs/
│   ├── PROJECT_SPEC.md
│   └── IMPLEMENTATION_PLAN.md
├── pyproject.toml             # uv project and test settings
├── uv.lock                    # Resolved Python dependencies
├── .gitignore
├── .env.example               # Variable names and placeholders
├── compose.yaml               # Local PostgreSQL and Airflow services
├── Dockerfile                 # Shared ETL dependencies in the Airflow image
├── src/renewflow/
│   ├── eia.py                 # Live API requests and response decoding
│   ├── transform.py           # UTC normalization and validation
│   ├── db.py                  # Transactional loading
│   └── cli.py                 # Sequential runner and backfill entry point
├── dags/
│   └── renewflow_daily.py     # Scheduling and task wrappers
├── sql/                       # Schema and analytical views
├── tests/                     # pytest checks and small sanitized fixtures
└── experiments/               # Capture, replay, and measurement entry points
```

Keep the Airflow metadata database and RenewFlow data in separate databases with separate roles on the same local PostgreSQL service. Start with Airflow `LocalExecutor` to exercise real task concurrency on one machine. Independent series can fetch in parallel, and distinct respondent-day batches write disjoint keys. Bound concurrent runs and live API requests. A task can pass a compact JSON-serializable payload for one validated series and day through XCom. The daily unit bounds each successful extraction to 24 observations. Retain snapshot files separately for repeatable experiments. [Airflow Local Executor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html), [Airflow XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)

## Phases and acceptance checks

| Phase | Deliverable | Acceptance check |
| --- | --- | --- |
| 0. Planning review | This plan, project specification, and contributor guidance | Review the data contract, experimental comparison, and open course requirements before coding. |
| 1. EIA input contract | Minimal project setup, ignored local config, shared Requests client, and a small credential-free historical fixture | Using a runtime EIA key, verify both routes and all three series for a known day. Check UTC alignment against source evidence, boundary semantics, units, and facets. Offline pytest covers response decoding, paging, and representative HTTP errors. |
| 2. Validation and normalization | Pandas transformation independent of Airflow and PostgreSQL | Cover midnight and a local daylight-saving transition, missing hours, malformed values, mismatched units or series, identical and conflicting duplicates, and the signed-value policy. A complete CISO day produces 24 unique slots per series. |
| 3. Storage and sequential baseline | Compose PostgreSQL, schema, shared psycopg load, and a Python command that processes explicit UTC dates one day at a time | Load a recorded day, rerun it, and compare key and value sets. Refresh one revised value, then test rollback after an injected write failure. Confirm a historical range processes the expected daily windows. |
| 4. SQL analytics | Hourly and daily views plus a reproducible local chart or export covering energy profiles | Verify time buckets, known sums, peak demand, and the ratio of totals using a fixture. Produce an hourly comparison and a seasonal or daily profile from real history with coverage counts. |
| 5. Airflow and recovery | Compose Airflow 3.x, daily DAG, per-attempt telemetry in both runners, and parameterized backfills | Verify three independently executable extract tasks and a successful daily load under the running scheduler. Test the 19:00 UTC mapping for scheduled, delayed, and retried runs. Backfill several UTC days, compare with sequential output, and demonstrate a transient failure recovering without duplicate rows. |
| 6. Experiments and demo | Snapshot manifest, benchmark runner, raw outputs, figures, and local walkthrough | Execute the paired experiments, confirm output equivalence and failure behavior, and rehearse the submitted code on a clean local setup. Verify the demo can replay with no EIA key after images and the snapshot are available. |

### Phase 1 development commands

Use uv with Python 3.12, Requests for extraction, and pytest for offline tests. Configure `EIA_API_KEY` in the ignored `.env` using `.env.example` as a template. Run `uv run --locked pytest` to exercise request mapping, pagination, errors, and snapshot redaction without EIA. Run the authenticated smoke check and capture from the repository root:

```sh
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15 \
  --snapshot tests/fixtures/eia-ciso-2025-01-15.json
```

The smoke check requires 24 ordered UTC hour endings and the expected unit for each series before it writes the snapshot. It reports observed counts, bounds, and units for review. Retain the generated fixture once the smoke check passes. Confirm any additional technology choices with the project owner before adopting them.

The live smoke run confirmed 24 observations per series, the final midnight boundary, and `megawatthours`. The [real capture](../tests/fixtures/eia-ciso-2025-01-15.json) is available for offline replay. All 11 extraction tests passed under `unittest` using the locked runtime dependencies.

### Phase 2 validation

Call `normalize_series(metric, utc_date, rows)` from `renewflow.transform` with the rows from the extractor or replay fixture. The result contains 24 sorted observations with UTC-aware hour starts and exact `Decimal` MWh values, plus input, duplicate, and negative generation counts. A `ValidationError` prevents later publication of an invalid or incomplete day.

Run `uv run --locked pytest` to check the real capture and validation cases offline after installing dependencies. The full suite passed, and the README replay validated all 72 observations from the real capture.

### Phase 3 PostgreSQL and sequential runner

See the [local PostgreSQL demo](../README.md#run-the-local-postgresql-demo) for database setup, fixture replay, and row-count verification. The runner reuses extraction and validation for live UTC date ranges and writes one transaction per complete day. Integration tests cover reruns, corrections, and transactional rollback against an isolated test database.

The unit suite and Compose configuration checks passed. The PostgreSQL integration tests still need a running test database.

### Phase 5 scheduling

For phase 5, use an explicit UTC cron timetable at `0 19 * * *` and derive the observation date from `data_interval_end` for scheduled and native backfill runs. Proposed default is `CronDataIntervalTimetable` with `catchup=False` and a fixed, UTC-aware start date that allows the chosen historical windows. The CLI accepts `[start_date, end_date)`. For native Airflow backfills, document the translation from that date range to the pinned version's logical-date bounds and verify it with a dry run and a three-day integration run. A manual single-day run takes an explicit observation date. [Airflow timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html), [Airflow backfills](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)

### Optional cloud phase

After the local deliverables work, an optional cloud phase can expose sanitized attempt events in PostgreSQL and connect Grafana Cloud through PDC. Give Grafana a scoped read-only role for the energy views and pipeline metrics. Evaluate EC2 separately if continuous operation is useful. [Grafana Cloud PostgreSQL and PDC setup](https://grafana.com/docs/learning-paths/postgresql-data-source/)

## Experiment protocol

1. Capture a historical EIA snapshot once. Strip echoed request credentials and retain the response fields needed for replay. Record routes, filters, capture date, periods, row and byte counts, and checksums. Both runners replay the same responses through the normal parsing, validation, and loading paths. Labeled test faults can modify transport or execution behavior while the underlying observations remain real.
2. Start with nested CISO windows of 1, 7, 30, and 180 days, contingent on availability and run time. Declare a selection rule and document gaps and exclusions before timing. Use the same day-sized work units in both runners. If a longer range or additional respondents is needed, record the actual sample size, data quality, and resulting scope separately from the CISO demo.
3. Use the same host, PostgreSQL settings, ETL dependency versions, starting database state, and input order definition. Warm services before timing, run several paired trials with alternating runner order, and log CPU and memory limits, Airflow concurrency, and database reset procedure. Compare sequential Python, Airflow with one active task slot, and Airflow with bounded parallelism. The latter two configurations help distinguish scheduler overhead from concurrency gains.
4. Time from run submission or CLI launch to terminal outcome and report wall time, per-stage duration, and successfully published observations per second. Report first-attempt success per comparable task, eventual success per requested daily unit, retries, recovery time from the first recorded failure, and work repeated. Retain raw run outcomes and report median and spread with sample counts. Separate controlled replay results from any live EIA timings. Use PostgreSQL query timing or resource measurements if needed for the course rubric.
5. Use deterministic faults in matched stages. Exercise a transient extract failure with equivalent attempt budgets and backoff, a permanent data-quality failure that leaves the target day unmodified, and an interrupted run after some stages succeed. Record the recovery mechanism used by each runner, including Airflow task retry or resume and sequential CLI rerun. Check database contents and equivalent hourly and daily results against a clean run before comparing performance.

This experiment measures day-level and series-level parallel work on one host. Use the recorded task count, row count, and resource limits when interpreting the results and discussing where network, PostgreSQL, or scheduler overhead may dominate.

## Open decisions and assumptions

| Decision | Proposed default or evidence needed |
| --- | --- |
| Late data and revisions | Keep strict completeness and bounded retries, then explicitly backfill failed days. Profile historical coverage and revisions before deciding whether a rolling refresh window is justified. |
| Signed generation in analytics | Preserve and count negative generation during validation. Profile its frequency and size over the benchmark window when interpreting generation totals and ratios. |
| Local runtime and retry settings | Pin a compatible Airflow 3.x, Python, PostgreSQL, and dependency set during setup. Confirm the group can run it on its demo machines. Set baseline and Airflow retry budgets after verifying rate limits, response delays, and scheduler behavior. |
| Academic minimum standards | Confirm the approved topic title and check the separate Minimum Required Standards for any requirement for ML, unstructured inputs, larger workloads, or distributed execution. The current plan uses justified descriptive analytics, semi-structured EIA data, a relational warehouse, and single-host task parallelism. |
| Benchmark scale and dates | Select snapshot dates, repetitions, parallel settings, and any extra respondent coverage after the coverage and run-time pilot. Retain actual day, row, and byte counts. |

## Submission readiness

Target a complete local rehearsal and code freeze by **November 30, 2026**, as a proposed internal milestone. The demo ZIP is due on **December 1, 2026, at 11:59 PM** via D2L. The in-person demo is **December 2, 2026, 8:00–9:50 AM in ENA 305** and assesses working code and each member's ability to execute and explain their own contribution. Name the ZIP using `Group Number SENG550 ProjectTitle Demo` with the approved title and group number. Include source and supporting files needed for the demo, plus setup and verification instructions.

For the December 4 report and recording, retain the measured outputs, source citations, architectural rationale, and individual contribution notes from the implementation. OpenAI Codex assisted with repository inspection, documentation research, planning, the extractor and validator, the sequential runner and PostgreSQL loader, their tests, and the local setup instructions. Each contributor should record the AI assistance and external sources applicable to their own work and independently verify the material they use or present.
