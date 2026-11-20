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
| 5. Airflow and recovery | Compose Airflow 3.x, daily DAG, shared multi-day replay, per-attempt telemetry in both runners, and parameterized backfills | Verify three independently executable extract-and-validate tasks and a successful daily load under the running scheduler. Test the 19:00 UTC mapping for scheduled, delayed, and retried runs. Replay the same three observation dates through both runners, compare stored rows and analytics, and recover from a transient fault without duplicate rows. |
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

### Phase 4 energy analytics

Apply [the hourly and daily views](../sql/analytics.sql) after the observations schema. Use the [CSV export instructions](../README.md#export-energy-analytics) to reproduce hourly, daily, and [seasonal summaries](../sql/seasonal_profile.sql) from loaded EIA data. Integration checks cover UTC grouping from a Pacific time session, known fixture totals, peak demand, the ratio of daily totals, and exclusion of incomplete days from the seasonal profile. Run these checks once a PostgreSQL test database is available.

### Phase 5 scheduling

For phase 5, use an explicit UTC cron timetable at `0 19 * * *` and derive the observation date from `data_interval_end` for scheduled and native backfill runs. Proposed default is `CronDataIntervalTimetable` with `catchup=False` and a fixed, UTC-aware start date that allows the chosen historical windows. The CLI accepts `[start_date, end_date)`. For native Airflow backfills, document the translation from that date range to the pinned version's logical-date bounds and verify it with a dry run and a three-day integration run. A manual single-day run takes an explicit observation date. [Airflow timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html), [Airflow backfills](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)

### Optional cloud phase

Hosted PostgreSQL deployment is deferred until near the end of the project, if needed. The local Docker Compose database remains the development and demo target. Running the PostgreSQL integration suite is still an open acceptance check. A local test database can satisfy that check ahead of hosted deployment.

After the local deliverables work, an optional cloud phase can expose sanitized attempt events in PostgreSQL and connect Grafana Cloud through PDC. Give Grafana a scoped read-only role for the energy views and pipeline metrics. Evaluate EC2 separately if continuous operation is useful. [Grafana Cloud PostgreSQL and PDC setup](https://grafana.com/docs/learning-paths/postgresql-data-source/)

## Experiment protocol

### Compare the same work

The Python CLI already supports a live date range and one saved UTC day. Its current tests cover range boundaries and stopping before an incomplete day is loaded. Airflow, multi-day replay, common telemetry, and the benchmark runner still need to be built. PostgreSQL integration checks for upserts and rollback need a running test database.

- Share one snapshot reader keyed by UTC date. The sequential CLI iterates the manifest in date order, and each Airflow run reads the entry for its observation date. Both use the same per-series validation and transactional loader. For timing, replay reads saved response data locally. Report live EIA timing separately because it includes network and provider variability.
- Treat solar, wind, and demand extraction with validation, plus the daily PostgreSQL load, as the four comparable stages. Python runs them in order. Airflow may run the three series concurrently and waits for all to validate before loading. Compare committed `(respondent, hour_start_utc, metric, value_mwh)` rows and hourly and daily SQL outputs for identical inputs.
- Run three configurations on the same host and pinned ETL environment. Use sequential Python, Airflow with one active task slot, and Airflow with bounded parallelism. Warm services beforehand, start each trial with the same observation-table state, and use isolated benchmark targets. Record versions, resource limits, Airflow task slots, and active DAG run limits. Alternate trial order and retain every result with its configuration.
- Pilot nested CISO windows of 1, 7, and 30 days. Attempt 180 days after the pilot establishes that repeated runs fit the project schedule. Declare how dates were chosen, report gaps and exclusions, and record actual days, rows, and input bytes. Capture the EIA data once, strip credentials, and record routes, filters, capture time, API version, and checksums in the manifest.

### Measure outcomes

| Course criterion | Evidence to record |
| --- | --- |
| Execution time | End-to-end wall time from CLI launch or benchmark submission until the requested workload reaches a terminal outcome. Include queue and scheduling time in the Airflow end-to-end result. Keep individual stage durations for interpretation. |
| Task reliability | First-attempt successes divided by comparable stages attempted. Also report days successfully published divided by days requested, attempts, retries, and blocked stages. Label live smoke outcomes and controlled fault outcomes by source. |
| Failure recovery | Time from the first recorded failure or interruption to verified publication, plus stages re-executed and whether recovery required operator action. Report terminal failures as outcomes with no recovery duration. |
| Processing scalability | Wall time and committed observations per second by workload size and runner configuration. Calculate throughput from successfully published observations in clean trials and report median, spread, and trial count. Discuss single-host concurrency and the effect of small CISO daily batches. |

Log comparable stage and attempt records from both paths with a trial ID, date, stage, attempt number, status, start and end times, duration, row counts, and sanitized error category. Use Airflow task/run metadata for scheduling and terminal state, and record unfinished attempts after interruption with unavailable fields left empty. Keep raw results so summaries can be regenerated. [Airflow public REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

Use the same historical inputs and inject faults at the same stage and date for each runner. Run the fault comparisons separately from clean timing trials.

1. Fail one extract attempt transiently. Give each runner the same retry budget and delay for this controlled comparison. Add stage-local retry handling to the sequential runner as needed and use Airflow's task retry policy for the DAG. Verify the final rows match the clean run.
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

## Submission readiness

Target a complete local rehearsal and code freeze by **November 30, 2026**, as a proposed internal milestone. The demo ZIP is due on **December 1, 2026, at 11:59 PM** via D2L. The in-person demo is **December 2, 2026, 8:00–9:50 AM in ENA 305** and assesses working code and each member's ability to execute and explain their own contribution. Name the ZIP using `Group Number SENG550 ProjectTitle Demo` with the approved title and group number. Include source and supporting files needed for the demo, plus setup and verification instructions.

For the December 4 report and recording, retain the measured outputs, source citations, architectural rationale, and individual contribution notes from the implementation. OpenAI Codex assisted with repository inspection, documentation research, planning, the extractor and validator, the sequential runner and PostgreSQL loader, the SQL analytics and exports, their tests, and the local setup instructions. Each contributor should record the AI assistance and external sources applicable to their own work and independently verify the material they use or present.
