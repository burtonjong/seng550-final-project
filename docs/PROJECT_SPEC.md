# RenewFlow project specification

## Objective and scope

RenewFlow evaluates how Airflow orchestration affects execution time, task reliability, failure recovery, and scalability relative to sequential Python. Both runners process identical historical EIA-930 observations and publish validated data to PostgreSQL. SQL summaries demonstrate the pipeline output and help verify equality.

The local submission uses Docker Compose, Airflow 3.x, Python, Pandas, PostgreSQL, psycopg, and pytest. Grafana Cloud, Private Data Source Connect, and EC2 deployment are optional follow-up work. [Architecture](ARCHITECTURE.md)

## Inputs and data characteristics

| File | Fields used |
| --- | --- |
| EIA-930 Balance | Authority code, UTC end of hour, reported demand, reported solar and wind net generation without integrated storage |
| EIA-930 Subregion | Authority code, string load-zone code, UTC end of hour, reported demand |

Over a one-hour interval, hourly average MW is numerically equal to MWh. Preserve reported values and signed solar or wind net generation. A nonzero integrated-storage contribution triggers an import error so its effect can be reviewed before changing the generation series definition. [EIA-930 methodology and data downloads](https://www.eia.gov/electricity/930-content/about.html)

| Dimension | Implication for RenewFlow |
| --- | --- |
| Volume | The four source files hold more than 1.25 million rows. The largest verified cohort processes 384,768 observations after filtering to selected authorities, zones, and complete UTC days. |
| Velocity | Observations are hourly. Historical files support repeatable manual replay and extensions as new file periods arrive. |
| Variety | Balance and Subregion have different schemas and identifiers. UTC normalization aligns their time axis while retaining authority and zone keys. |
| Veracity | Missing observations, revisions, conflicting duplicates, and local reporting anomalies affect coverage. The manifest records the source hashes and the excluded dates with reasons. |
| Value | Hourly matching supports analysis of renewable generation, demand peaks, and zone-level demand. Stage telemetry shows where orchestration time and retry work occur. |

## UTC replay and publication contract

- The observation interval begins at 00:00 UTC and ends at midnight at the start of the next date. The input hour ending at that midnight belongs to the preceding UTC day. Normalize to `hour_start_utc` by subtracting one hour.
- The preparation command identifies candidate days, validates their coverage, and records the exact shared benchmark dates without generating benchmark snapshots. Benchmark Extract, Transform, and Load use the same ordered dates, even across gaps. The one-day demo accepts a manual observation date and a replay manifest.
- Transform checks all expected timestamps, series and zone identifiers, units, finite numeric values, and duplicates. Authority demand must be positive. Zone demand can be zero. Collapse identical duplicates and reject conflicting values and incomplete days.
- Store observations with keys `(respondent, hour_start_utc, metric)` and `(respondent, hour_start_utc, subregion)`. Transactional upserts apply changed values. Application loads skip an authority-day whose validated content matches the committed checkpoint. Benchmark trials use fresh schemas and run the full write path. [Application setup and replay](BACKFILLS.md)

## Analytics and monitoring

- The hourly SQL view aligns complete solar, wind, and demand records per respondent and UTC interval. It shows the three values, combined solar and wind, the ratio to positive demand, and demand less solar and wind. Label the ratio as **solar and wind generation relative to demand**. Imports and other generation sources are outside that measure.
- The daily SQL view groups aligned hours by UTC date. It shows energy totals in MWh, the number of complete hours, peak hourly demand, and the ratio of total solar and wind to total demand. The seasonal CSV groups complete UTC days by season and hour. It reports the mean for each series, the day count, and the first and last contributing dates so claims reflect coverage.
- Capture structured run and attempt records for both runners, with run ID, target date, stage, attempt number, status, UTC start and finish, duration, relevant row counts, and sanitized error category. Use the Airflow UI and local log exports during the demo. Export performance data for plots. For a terminated task that cannot emit a finish event, record the outcome from the runner and leave unavailable timing fields empty.

## Experimental acceptance

One experiment compares sequential Python with Airflow at one and four task slots. Four authority chains each use a separate extract, transform, and load task across the 1-, 7-, 30-, and 334-day workloads selected from the 2025 CSVs. Both runners scan the same original EIA files once per authority inside the timed Extract stages. They use the same intermediate files and database operations. Record task overlap, end-to-end wall time, and verified rows per second. Test whether four-slot Airflow overtakes Python as each task gets more rows.

In separate recovery trials, fail CISO’s load immediately after three UTC days commit. Both runners automatically retry once using staged transformed data and idempotent writes. Measure recovery time, repeated work, first-attempt success, and operator intervention. Verify all SQL outputs against clean runs. [Benchmark guide](BENCHMARKS.md)

Evaluate execution time, task reliability, failure recovery, and processing scalability. Require output equivalence and transactional integrity before interpreting speed. Discuss source-quality exclusions, scheduling overhead, and the scope of single-host task parallelism. The [experiment protocol](IMPLEMENTATION_PLAN.md#experiment-protocol) defines the controls, metrics, and evidence needed for the IEEE report.
