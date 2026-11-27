CREATE TABLE IF NOT EXISTS hourly_observations (
    respondent text NOT NULL,
    hour_start_utc timestamptz NOT NULL,
    metric text NOT NULL CHECK (metric IN ('solar', 'wind', 'demand')),
    value_mwh numeric NOT NULL,
    PRIMARY KEY (respondent, hour_start_utc, metric)
);

CREATE TABLE IF NOT EXISTS renewflow_completed_days (
    respondent text NOT NULL,
    utc_date date NOT NULL,
    observation_digest text NOT NULL,
    full_digest text,
    PRIMARY KEY (respondent, utc_date)
);
