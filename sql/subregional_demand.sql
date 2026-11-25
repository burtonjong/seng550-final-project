CREATE TABLE IF NOT EXISTS hourly_subregional_demand (
    respondent text NOT NULL,
    hour_start_utc timestamptz NOT NULL,
    subregion text NOT NULL,
    value_mwh numeric NOT NULL CHECK (value_mwh >= 0),
    PRIMARY KEY (respondent, hour_start_utc, subregion)
);

CREATE OR REPLACE VIEW daily_subregional_demand AS
SELECT
    respondent,
    subregion,
    (hour_start_utc AT TIME ZONE 'UTC')::date AS utc_date,
    count(*) AS observed_hours,
    sum(value_mwh) AS demand_mwh,
    max(value_mwh) AS peak_hourly_demand_mwh
FROM hourly_subregional_demand
GROUP BY respondent, subregion, (hour_start_utc AT TIME ZONE 'UTC')::date;
