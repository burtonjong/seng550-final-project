# RenewFlow contributor guide

- Read [PROJECT_SPEC.md](docs/PROJECT_SPEC.md) before changing ingestion, time windows, data quality, storage, analytics, or experiment behavior.
- Read [IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) before starting a phase, choosing a dependency, or collecting demo and benchmark evidence. Read [ARCHITECTURE.md](docs/ARCHITECTURE.md) when changing how stages exchange data or publish it. Begin implementation after the planning review and use the phase acceptance checks to verify each increment.
- Keep the ETL functions runnable from plain Python. Put Airflow scheduling and task adapters in `dags/` and reuse the same extraction, validation, and loading code in the sequential runner.
- Pass explicit UTC windows to ETL functions and keep PostgreSQL writes idempotent. Cover midnight boundaries, malformed data, reruns, and rollback with pytest as those behaviors are added.
- Keep credentials in runtime configuration. Check logs, fixtures, exports, and submission archives for secrets.
- Retain provenance for data and experimental outputs. Report measured results with their inputs and settings. Record the AI tool, purpose, affected work, and independent verification so each contributor can disclose and explain their own work.
