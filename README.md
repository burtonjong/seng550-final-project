# SENG 550 final project

This project collects hourly solar generation, wind generation, and electricity demand for the California ISO from the U.S. Energy Information Administration (EIA).

## Set up the project

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run these commands from the repository root. The first setup needs network access to download Python and dependencies.

```sh
uv python install 3.12
uv sync --locked
```

`uv python install` makes Python 3.12 available. `uv sync --locked` creates `.venv` and installs the project and test dependencies from `uv.lock`. Run project commands through `uv run` to use that environment.

## Run the local demo offline

After setup, replay the [captured EIA data](tests/fixtures/eia-ciso-2025-01-15.json). This exercises the Pandas validator with real observations and needs neither an EIA key nor a running database. `--offline` tells uv to use the installed and cached dependencies.

```sh
uv run --locked --offline python - <<'PY'
import json
from datetime import date
from pathlib import Path

from renewflow.transform import normalize_series

path = Path("tests/fixtures/eia-ciso-2025-01-15.json")
capture = json.loads(path.read_text(encoding="utf-8"))
day = date.fromisoformat(capture["utc_date"])
total = 0

for metric, series in capture["series"].items():
    result = normalize_series(metric, day, series["response"]["data"])
    hours = result.observations["hour_start_utc"]
    total += len(hours)
    print(f"{metric}: {len(hours)} hours from {hours.iloc[0]} through {hours.iloc[-1]}")
    print(f"  duplicates collapsed: {result.identical_duplicates}")
    print(f"  negative generation hours: {result.negative_generation_hours}")

print(f"Total validated observations: {total}")
PY
```

Expect **72 observations**, with 24 per series from `00:00` through `23:00` UTC. EIA labels each reading by the hour ending, so the following midnight becomes `23:00` on the operating day. The fixture includes **14 negative solar readings**, which the validator preserves and counts.

## Run the tests

```sh
uv run --locked pytest
```

This runs the full suite, including mocked HTTP failures and pagination, replay of the real capture, and validation of timestamps, missing hours, duplicates, and values. The tests make no live EIA calls. After `uv sync --locked`, you can also run them with `uv run --locked --offline pytest`.

## Run a live EIA check

This optional check needs a network connection to `api.eia.gov` and an [EIA API key](https://www.eia.gov/opendata/register.php). If `.env` is missing, create it from the template and put your key in its `EIA_API_KEY` entry. `.env` is ignored by Git.

```sh
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15
```

`--env-file .env` loads the key, and `--date` selects the UTC operating day. The check requests all three series and reports counts, first and last EIA timestamps, and units. For this date, each series should show 24 hour endings from `2025-01-15T01` through `2025-01-16T00` and units of megawatthours.

To save a fresh sanitized capture, use `--snapshot` with a new path. `mktemp -d` creates a temporary directory so the command can be repeated without replacing an existing fixture.

```sh
snapshot_dir=$(mktemp -d)
uv run --locked --env-file .env python -m renewflow.eia --date 2025-01-15 \
  --snapshot "$snapshot_dir/eia-ciso-2025-01-15.json"
```

## Project references

- [Project specification](docs/PROJECT_SPEC.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)
- [Airflow pipeline tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html)
