# Testing RenewFlow

Run commands from the repository root after [project setup](../README.md).

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

These tests use isolated schemas in `renewflow_test` to check reruns, revisions, rollback, and SQL analytics. To run the full pytest suite, use `uv run --locked --env-file .env pytest`.

## Run a live EIA check

This optional check needs a network connection to `api.eia.gov` and an [EIA API key](https://www.eia.gov/opendata/register.php). Set `EIA_API_KEY` in your ignored `.env` file.

```sh
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15
```

`--env-file .env` loads the key, and `--date` selects the UTC operating day. The check requests all three series and reports counts, first and last EIA timestamps, and units. For this date, each series should show 24 hour endings from `2025-01-15T01` through `2025-01-16T00` and units of megawatthours.

The final lines confirm the total number of observations and tell you whether the output was a terminal summary or a saved JSON file. To save a fresh sanitized capture, use `--snapshot` with a new path. `mktemp -d` creates a temporary directory so the command can be repeated without replacing an existing fixture.

```sh
snapshot_dir=$(mktemp -d)
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15 \
  --snapshot "$snapshot_dir/eia-ciso-2025-01-15.json"
```
