# Testing RenewFlow

Run commands from the repository root after [project setup](../README.md).

## Unit checks

```sh
uv run --locked pytest -m "not postgres"
```

The tests exercise CSV extension and overlaps, coverage gaps, UTC boundaries, replay, and staged recovery. Small fixtures supply deterministic input. The DAG structure check runs when the Airflow SDK is installed.

## PostgreSQL integration tests

Start local PostgreSQL through Compose and create the test database once. Configure `POSTGRES_PASSWORD` in `.env` first. [Local setup](../README.md#install-and-configure)

```sh
docker compose up -d --wait postgres
docker compose exec -T postgres sh -c 'createdb -U "$POSTGRES_USER" renewflow_test'
docker compose port postgres 5432
```

Set `RENEWFLOW_TEST_DATABASE_URL` in the same `.env`. The default host port is `5432`, so the default URL is `postgresql://renewflow:YOUR_LOCAL_PASSWORD@127.0.0.1:5432/renewflow_test`. If you set `POSTGRES_PORT=5433` to avoid a conflict, use `5433` in the URL. The `5432` in `docker compose port postgres 5432` refers to the container port, and the command prints the host port to use. Use your actual credentials and percent-encode special characters in the password if needed. Reuse `renewflow_test` if it already exists.

```sh
uv run --locked --env-file .env pytest -m postgres
```

These tests use isolated schemas in `renewflow_test` to check reruns, revisions, checkpoint rollback, and SQL analytics. To run the full pytest suite, use `uv run --locked --env-file .env pytest`.
