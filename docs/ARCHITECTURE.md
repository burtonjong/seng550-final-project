# RenewFlow architecture

RenewFlow runs on one machine. Docker Compose starts PostgreSQL and can enable Airflow with LocalExecutor. Sequential Python and Airflow share the transformation and transactional load functions. The experiment measures the cost and benefit of orchestration on this local setup.

```mermaid
flowchart LR
    CSV["EIA-930 Balance and Subregion CSVs"] --> Prep["Coverage and provenance check"]
    Prep --> Manifest["Direct CSV benchmark manifest"]
    CSV --> Extract["Timed authority Extract"]
    Manifest --> Extract
    Extract --> Transform["Transform staged authority-day rows"]
    Transform --> Benchmark["Load benchmark trial schemas"]
    Benchmark --> Reports["SQL checksums and performance reports"]
```

## Data contracts

| Boundary | Data and responsibility |
| --- | --- |
| CSV → workload manifest | The preparation command checks both CSV families and records eligible UTC dates, zone codes, input sizes, and hashes. The benchmark uses the manifest and original files. |
| Extract → Transform | Each benchmark Extract task scans the original CSVs and stages the rows for its authority and the selected UTC dates. It passes the raw staging reference to Transform. |
| Transform → Load | Transform checks identifiers, periods, values, duplicate hours, and complete UTC coverage. It emits normalized authority-wide and zone rows to staged JSON files and passes the normalized staging reference to Load. |
| Load → analytics | One PostgreSQL transaction writes both sets of rows for an authority-day. The primary keys use authority, UTC hour start, and metric or zone. The SQL views aggregate committed observations. |

The benchmark DAG has four independent authority chains, each with one Extract, Transform, and Load task. The sequential benchmark executes the same chains in order. Airflow can overlap ready tasks from different chains with four slots. XCom carries small staging references for this DAG.

## Transactions and recovery

- A benchmark trial uses a fresh schema in `renewflow_benchmark`. The controlled failure occurs after the third selected CISO day commits. Its retry reuses the staged Transform output and revisits committed days with idempotent upserts. The harness verifies output equality and records the failed and successful attempts.
- Airflow run and task state live in the Airflow metadata database. Airflow metadata and benchmark trial schemas use separate databases on the same local PostgreSQL service.

[Benchmark protocol](BENCHMARKS.md)
