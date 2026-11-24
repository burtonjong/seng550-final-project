# RenewFlow implementation plan

## Current repository and proposed layout

The project directory was empty at initial inspection. Create the paths below as their phases need them.

```text
renewflow/
├── AGENTS.md
├── README.md                  # Local setup and links to run guides
├── docs/
│   ├── PROJECT_SPEC.md
│   ├── IMPLEMENTATION_PLAN.md
│   ├── TESTING.md
│   ├── BACKFILLS.md
│   ├── LOCAL_DEMO.md
│   └── BENCHMARKS.md
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
| 5. Airflow and recovery | Compose Airflow 3.x, daily DAG, shared multi-day replay, per-attempt telemetry in both runners, and parameterized backfills | Verify three independently executable extract-and-validate tasks and a successful daily load under the running scheduler. Test the 19:00 UTC mapping for scheduled, delayed, and retried runs. Replay the same three observation dates through both runners, compare stored rows and analytics, and recover from a transient fault without duplicate rows. |
| 6. Experiments and demo | Snapshot manifest, benchmark runner, raw outputs, figures, and local walkthrough | Execute the paired experiments, confirm output equivalence and failure behavior, and rehearse the submitted code on a clean local setup. Verify that the demo fetches and loads a historical day directly from EIA with the configured key. |

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

See the [local PostgreSQL demo](LOCAL_DEMO.md) for database setup, live EIA ingestion, and row-count verification. The runner reuses extraction and validation for live UTC date ranges and writes one transaction per complete day. Integration tests cover reruns, corrections, and transactional rollback against an isolated test database.

The unit suite and Compose configuration checks passed. The PostgreSQL integration tests still need a running test database.

### Phase 4 energy analytics

Apply [the hourly and daily views](../sql/analytics.sql) after the observations schema. Use the [CSV export instructions](LOCAL_DEMO.md#export-energy-analytics) to reproduce hourly, daily, and [seasonal summaries](../sql/seasonal_profile.sql) from loaded EIA data. Integration checks cover UTC grouping from a Pacific time session, known fixture totals, peak demand, the ratio of daily totals, and exclusion of incomplete days from the seasonal profile. Run these checks once a PostgreSQL test database is available.

### Phase 5 scheduling

The default Compose startup runs PostgreSQL for the sequential baseline. The `airflow` profile enables the scheduler, DAG processor, and API server alongside it. [Docker Compose profiles](https://docs.docker.com/compose/how-tos/profiles/)

The Airflow 3.3.2 DAG uses `CronDataIntervalTimetable` at `0 19 * * *` UTC with `catchup=False`. Scheduled and backfill runs derive the observation date from `data_interval_end` and manual runs take an explicit date. The [local Airflow instructions](BACKFILLS.md) document the logical-date translation, shared replay manifest, logging, and a controlled transient retry. Both runners allow two extract retries at a five-second delay. Airflow records run-level state in its metadata and the Python CLI emits a run summary alongside the shared stage records. [Airflow timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html), [Airflow backfills](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)

Host-side tests cover the UTC date rule and three-day sequential replay with a controlled retry. The Docker image build, Airflow scheduler run, three-day output comparison, and Airflow retry under the scheduler still need verification on a machine with access to the Docker daemon.

### Optional cloud phase

The project uses local Docker Compose for development, integration tests, experiments, and the demo. Run the pending PostgreSQL integration suite against a test database in the local instance.

After the local deliverables work, an optional cloud phase can expose sanitized attempt events in PostgreSQL and connect Grafana Cloud through PDC. Give Grafana a scoped read-only role for the energy views and pipeline metrics. Evaluate EC2 separately if continuous operation is useful. [Grafana Cloud PostgreSQL and PDC setup](https://grafana.com/docs/learning-paths/postgresql-data-source/)

## Experiment protocol

### Phase 6 commands and demonstration

The benchmark guide documents [historical snapshot capture](BENCHMARKS.md#capture-historical-eia-snapshots) and [paper benchmarks](BENCHMARKS.md). One native Airflow backfill submission starts one independently retryable run per requested UTC day. The range adapter checks Airflow's logical dates before submitting, then waits for terminal state through the public API. Start the local demo with a direct EIA fetch of one historical UTC day and verify 72 stored observations. Use the captured data for the separate controlled wind retry and rehearse a seven-day backfill before presenting it.

Both runners use the same `.env` and local Compose stack as the demo. The benchmark harness creates `renewflow_benchmark` in the local PostgreSQL instance and uses a fresh schema per trial. It compares committed rows with the expected normalized replay data and records hashes for the hourly and daily views. The suite and report check matching output hashes across runners. Airflow timing includes submission, queueing, and completion polling. Capturing inputs, preparing the schema, fetching task attempts, and comparing results happen outside the timed interval. The report uses Matplotlib for its PDF and writes CSV tables from the recorded trials.

### Compare the same work

The two runners share the UTC date and replay conventions, validation and loading functions, and per-stage telemetry. The tools for matched measurements are implemented. Actual measurement artifacts, the Airflow scheduler acceptance checks, and the PostgreSQL integration checks need a running local stack.

- Share one snapshot reader keyed by UTC date. The sequential CLI iterates the manifest in date order, and each Airflow run reads the entry for its observation date. Both use the same per-series validation and transactional loader. For timing, replay reads saved response data locally. Report live EIA timing separately because it includes network and provider variability.
- Treat solar, wind, and demand extraction with validation, plus the daily PostgreSQL load, as the four comparable stages. Python runs them in order. Airflow may run the three series concurrently and waits for all to validate before loading. Compare committed `(respondent, hour_start_utc, metric, value_mwh)` rows and hourly and daily SQL outputs for identical inputs.
- Run sequential Python, Airflow with one task slot, and Airflow with four task slots on the same host with pinned ETL dependencies. The Python runner starts as a host process and Airflow runs in Docker. Document those environments, resource limits, Airflow task slots, and active-run limits. Warm services beforehand and start each trial with the same empty observation-table state. Rotate trial order and retain each result.
- Pilot nested CISO windows of 1, 7, and 30 days with five trials per configuration. Attempt 365 and 730 days with three trials per configuration if verified coverage and the pilot permit. Declare how dates were chosen, report gaps and unavailable cohorts, and record actual days, rows, and input bytes. Capture once, strip credentials, and record routes, filters, capture time, API version, and checksums.

### Measure outcomes

| Course criterion | Evidence to record |
| --- | --- |
| Execution time | End-to-end wall time from CLI launch or benchmark submission until the requested workload reaches a terminal outcome. Include queue and scheduling time in the Airflow end-to-end result. Keep individual stage durations for interpretation. |
| Task reliability | First-attempt successes divided by comparable stages attempted. Also report days successfully published divided by days requested, attempts, retries, and blocked stages. Label live smoke outcomes and controlled fault outcomes by source. |
| Failure recovery | Time from the first recorded failure or interruption to verified publication, plus stages re-executed and whether recovery required operator action. Report terminal failures as outcomes with no recovery duration. |
| Processing scalability | Wall time and committed observations per second by workload size and runner configuration. Calculate throughput from successfully published observations in clean trials and report median, spread, and trial count. Discuss single-host concurrency and the effect of small CISO daily batches. |

Log comparable stage and attempt records from both paths with a trial ID, date, stage, attempt number, status, start and end times, duration, row counts, and sanitized error category. Use Airflow task/run metadata for scheduling and terminal state, and record unfinished attempts after interruption with unavailable fields left empty. Keep raw results so summaries can be regenerated. [Airflow public REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

Use the same historical inputs and inject faults at the same stage and date for each runner. Run the fault comparisons separately from clean timing trials.

1. Fail one extract attempt transiently. Use the same replay fault target and shared retry count and delay. Verify the final rows match the clean run.
2. Remove an expected hour in a labeled copy of the replay payload. Verify both runners report the validation failure and preserve any previously committed batch for that date.
3. Interrupt a run after a successful stage and before the daily commit. Record which stages Airflow can reuse and which the sequential CLI reruns. Use the same recovery procedure timing across trials and verify no partial or duplicate rows remain.

### Retain evidence for the IEEE report

- **Methods and implementation.** Save the architecture and ETL flow diagrams, schema and SQL, the snapshot manifest, exact runner commands, and the experimental configuration. Use the approved topic title and the IEEE conference template provided on D2L.
- **Results and discussion.** Retain raw run and attempt records, output equality checks, a runtime and throughput plot by workload size, and a table for reliability and recovery. Include an energy analytics figure from the hourly or daily views. Use units, trial counts, captions, and references for every figure or table.
- **References and appendix.** Cite at least five relevant scholarly or technical references in IEEE style. The project specification and plan link primary EIA, Airflow, and PostgreSQL references for the implemented methods. Retain each group member's contribution statement, AI-use disclosure, and independent verification notes. Base the abstract and conclusions on completed experiments for the report's five-page minimum excluding the appendix.

## Open decisions and assumptions

| Decision | Proposed default or evidence needed |
| --- | --- |
| Late data and revisions | Keep strict completeness and bounded retries, then explicitly backfill failed days. Profile historical coverage and revisions before deciding whether a rolling refresh window is justified. |
| Signed generation in analytics | Preserve and count negative generation during validation. Profile its frequency and size over the benchmark window when interpreting generation totals and ratios. |
| Local runtime and retry settings | Pin a compatible Airflow 3.x, Python, PostgreSQL, and dependency set during setup. Confirm the group can run it on its demo machines. Set baseline and Airflow retry budgets after verifying rate limits, response delays, and scheduler behavior. |
| Academic minimum standards | Confirm the approved topic title and check the separate Minimum Required Standards for any requirement for ML, unstructured inputs, larger workloads, or distributed execution. The current plan uses justified descriptive analytics, semi-structured EIA data, a relational warehouse, and single-host task parallelism. |
| Benchmark scale and dates | Select snapshot dates, repetitions, parallel settings, and any extra respondent coverage after the coverage and run-time pilot. Retain actual day, row, and byte counts. |

## Remaining checks for the IEEE results

As of November 24, 2026, Phases 0–2 are complete and the core code for Phases 3–6 is in place. The local replay manifest has one verified historical day, January 15, 2025. There are no benchmark trial records or generated results in the workspace. The capture stopped at the next day after an EIA connection failure, and this workspace could not start the local Docker daemon.

1. **Run the local PostgreSQL tests (Phases 3–4).** Start the Compose database, configure `RENEWFLOW_TEST_DATABASE_URL`, and run the five PostgreSQL integration cases. Save the outcome for the storage and analytics verification. [Test commands](TESTING.md#postgresql-integration-tests)
2. **Verify Airflow under the scheduler (Phase 5).** Build and start Airflow. Run a one-day replay, then replay a shared multi-day range in both runners and confirm matching observations and analytics. Verify the scheduled date mapping in Airflow and rehearse a manual seven-day backfill with the recorded data. [Airflow setup and commands](BACKFILLS.md)
3. **Collect matched performance trials (Phase 6).** Capture and freeze 30 contiguous historical days, using the same start date for the 1-, 7-, and 30-day cohorts. For a complete cohort, run five trials each for sequential Python, Airflow with one slot, and Airflow with four slots. Keep the raw timings and verify the database checksums before interpreting performance. Attempt the 365- and 730-day tiers if verified coverage and the pilot time budget permit. Record why any tier was skipped. [Benchmark commands](BENCHMARKS.md#collect-matched-measurements)
4. **Collect reliability and recovery evidence (Phases 5–6).** Run the controlled wind retry once on each runner as a first check. Record attempts, repeated stages, time to publication, and final row equality. The benchmark CLI supports the transient retry; the missing-hour and interrupted-run scenarios in the protocol still require controlled execution and evidence. State which scenarios were run in the report. [Failure scenarios](#measure-outcomes)
5. **Generate the figures and tables (Phases 4 and 6).** Keep `timings.csv`, `reliability.csv`, `performance.pdf`, raw trial JSON, the snapshot manifest and coverage notes, and an example energy analytics export from PostgreSQL. Use the measured orchestration comparison for the Results section and one energy figure to illustrate the pipeline output. [Analytics export](LOCAL_DEMO.md#export-energy-analytics) · [Report generation](BENCHMARKS.md#generate-tables-and-figures)

## Submission readiness

Target a complete local rehearsal and code freeze by **November 30, 2026**, as a proposed internal milestone. The demo ZIP is due on **December 1, 2026, at 11:59 PM** via D2L. The in-person demo is **December 2, 2026, 8:00–9:50 AM in ENA 305** and assesses working code and each member's ability to execute and explain their own contribution. Name the ZIP using `Group Number SENG550 ProjectTitle Demo` with the approved title and group number. Include source and supporting files needed for the demo, plus setup and verification instructions.

For the December 4 report and recording, retain the measured outputs, source citations, architectural rationale, and individual contribution notes from the implementation. OpenAI Codex assisted with repository inspection, documentation research, planning, the extractor and validator, the sequential runner and PostgreSQL loader, SQL analytics, Airflow orchestration, replay and telemetry, benchmark and reporting tools, tests, and local setup instructions. Each contributor should record the AI assistance and external sources applicable to their own work and independently verify the material they use or present.
