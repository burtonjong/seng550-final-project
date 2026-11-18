CREATE TABLE IF NOT EXISTS hourly_observations (
    respondent text NOT NULL,
    hour_start_utc timestamptz NOT NULL,
    metric text NOT NULL CHECK (metric IN ('solar', 'wind', 'demand')),
    value_mwh numeric NOT NULL,
    PRIMARY KEY (respondent, hour_start_utc, metric)
);
