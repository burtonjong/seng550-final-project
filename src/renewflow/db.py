"""Store validated RenewFlow observations in PostgreSQL."""

import pandas as pd
import psycopg

_KEY = ["respondent", "hour_start_utc", "metric"]
_UPSERT = """
    INSERT INTO hourly_observations (respondent, hour_start_utc, metric, value_mwh)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (respondent, hour_start_utc, metric) DO UPDATE
    SET value_mwh = EXCLUDED.value_mwh
    WHERE hourly_observations.value_mwh IS DISTINCT FROM EXCLUDED.value_mwh
"""


def load_day(connection: psycopg.Connection, observations: pd.DataFrame) -> int:
    """Upsert one validated day and return the number of inserted or updated rows."""
    # Visit keys in the same order when more than one run writes the same day.
    batch = observations[_KEY + ["value_mwh"]].sort_values(_KEY)
    rows = (
        (respondent, hour_start.to_pydatetime(), metric, value_mwh)
        for respondent, hour_start, metric, value_mwh in batch.itertuples(index=False, name=None)
    )
    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(_UPSERT, rows)
        changed_rows = cursor.rowcount
    return changed_rows
