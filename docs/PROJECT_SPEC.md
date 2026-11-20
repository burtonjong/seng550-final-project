# RenewFlow project specification

## Objective and scope

RenewFlow is a SENG 550 project that collects hourly solar generation, wind generation, and demand for the California ISO balancing authority (`CISO`), publishes validated data to PostgreSQL, and evaluates Airflow against a sequential Python baseline. The research questions are how the solar and wind contribution varies across hours and seasons, and how workflow orchestration affects backfill throughput, reliability, and recovery as the number of daily work units grows.

The submission target is a repeatable local system using Docker Compose, Airflow 3.x, Python, Requests, Pandas, PostgreSQL, psycopg, and pytest. SQL summaries and local figures provide the analytics evidence. Grafana Cloud and Private Data Source Connect, and a potential always-on EC2 deployment, are optional follow-up work. The local demo includes a scheduled DAG configuration and can run the pipeline on saved historical data without cloud credentials. Live scheduled ingestion runs while the local Airflow services are up and an EIA key is configured.

## Inputs and data characteristics

| Series | EIA API v2 data route | Filters in addition to `respondent=CISO` |
| --- | --- | --- |
| Solar net generation | `electricity/rto/fuel-type-data/data/` | `fueltype=SUN` |
| Wind net generation | `electricity/rto/fuel-type-data/data/` | `fueltype=WND` |
| Actual demand | `electricity/rto/region-data/data/` | `type=D` |

Request the `value` column and the `hourly` frequency. Verify the offset-free `period` field's UTC interpretation, hour-ending behavior, response units, and API date bounds on both routes before finalizing the parser. Use stable sorting and honor pagination metadata so truncated results are detectable. Record the route, facets, capture time, and API version in snapshot provenance. [EIA API v2 technical documentation](https://www.eia.gov/opendata/documentation/APIv2.1.0.pdf)

| Dimension | Implication for RenewFlow |
| --- | --- |
| Volume | Three series yield up to 72 observations per complete CISO UTC day, or 26,280 per 365-day year. The experiment scales by replaying more real historical days and reports the actual rows and bytes processed. |
| Velocity | Observations are hourly and the pipeline runs daily. EIA publishes demand and generation on different timelines, so check all three series for completeness at load time. |
| Variety | Two semi-structured JSON response shapes use different metric identifiers. Normalize them into structured relational records. The proposed dataset scope covers tabular observations and their API envelopes. |
| Veracity | Late reports, revisions, anomalies, and daylight-saving reporting defects can affect coverage and values. Check the expected UTC slots and quantify defects in the selected history. |
| Value | Hourly alignment supports investigation of demand peaks and the contribution of solar and wind across the day. Attempt-level records support investigation of delays, failures, and recovery. |

EIA publishes observations using the hour-ending convention and documents reporting delays and revisions. Profile missing, duplicate, and anomalous values on the historical snapshot and retain the counts used to explain analytical coverage. [EIA-930 methodology and data quality notes](https://www.eia.gov/electricity/930-content/about.html)

## Scheduling and publication contract

- Schedule one Airflow run per day at **19:00 UTC**. Derive the target date from the run's scheduled time, including when it starts late or is retried. A run scheduled on November 18 at 19:00 UTC covers the operating hours in `[November 17 00:00, November 18 00:00)` UTC.
- Normalize to `hour_start_utc`. Under the source hour-ending convention, the example window needs EIA hour endings from November 17 01:00 through November 18 00:00 inclusive. Verify that the request and parser together select those exact 24 intervals, including the final hour ending at midnight. UTC windows always have 24 expected slots, including dates with local clock changes.
- Accept explicit UTC dates for historical reruns. Define date ranges as inclusive start and exclusive end, and split the range into daily work units. The Airflow and sequential entry points must resolve the same set of dates.
- Give solar, wind, and demand independent extraction tasks. Reuse a common fetch function and per-series validation in both runners. Join the three outputs before publication so a launch, retry, or backfill produces a complete daily batch.

## Quality and persistence

- Check response schema, series identifiers, units, hourly timestamp alignment, and bounds. Reject missing, nonnumeric, or nonfinite values and nonpositive CISO demand. Preserve signed net generation and count the hours with negative readings for review. The captured CISO fixture contained 14 negative solar readings in 24 hours. Profile the frequency and size of signed values over the historical benchmark window.
- Collapse duplicates with the same hour and equivalent numeric value. Count the duplicates and fail validation for conflicting values at the same hour, mismatched records, or missing expected hours. Surface the date, series, and counts so operators can retry or backfill. Publish only complete days and keep any previously published batch if an attempted refresh fails.
- Store long-form observations keyed by `(respondent, hour_start_utc, metric)` with `timestamptz` and numeric `value_mwh`. Publish all three series in one PostgreSQL transaction using a unique key and `INSERT ... ON CONFLICT DO UPDATE`. An identical replay preserves rows and values. A successful live refresh can replace revised values. Benchmark replay writes to a separate target. [PostgreSQL `INSERT` reference](https://www.postgresql.org/docs/current/sql-insert.html)

## Analytics and monitoring

- The hourly SQL view aligns complete solar, wind, and demand records per respondent and UTC interval. It shows the three values, combined solar and wind, the ratio to positive demand, and demand less solar and wind. Label the ratio as **solar and wind generation relative to demand**. Imports and other generation sources are outside that measure.
- The daily SQL view groups aligned hours by UTC date. It shows energy totals in MWh, the number of complete hours, peak hourly demand, and the ratio of total solar and wind to total demand. The seasonal CSV groups complete UTC days by season and hour. It reports the mean for each series, the day count, and the first and last contributing dates so claims reflect coverage.
- Capture structured run and attempt records for both runners, with run ID, target date, stage, attempt number, status, UTC start and finish, duration, relevant row counts, and sanitized error category. Use the Airflow UI and local log exports during the demo. Export performance data for plots. For a terminated task that cannot emit a finish event, record the outcome from the runner and leave unavailable timing fields empty.

## Experimental acceptance

Use the sequential Python runner as the baseline and the scheduled Airflow DAG as the orchestrated implementation. Both must perform the same three per-series extraction and validation stages and the same daily PostgreSQL load. Replay identical real historical EIA captures through the shared validation and loading functions. The daily job and the benchmark backfills must use the same observation date and publication rules.

Evaluate execution time, task reliability, failure recovery, and processing scalability. Require output equivalence and transactional integrity before interpreting speed. Discuss source-quality exclusions, scheduling overhead, and the scope of single-host task parallelism. The [experiment protocol](IMPLEMENTATION_PLAN.md#experiment-protocol) defines the controls, metrics, and evidence needed for the IEEE report.
