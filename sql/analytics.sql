CREATE OR REPLACE VIEW hourly_energy_summary AS
WITH aligned AS (
    SELECT
        respondent,
        hour_start_utc,
        MAX(value_mwh) FILTER (WHERE metric = 'solar') AS solar_mwh,
        MAX(value_mwh) FILTER (WHERE metric = 'wind') AS wind_mwh,
        MAX(value_mwh) FILTER (WHERE metric = 'demand') AS demand_mwh
    FROM hourly_observations
    GROUP BY respondent, hour_start_utc
    -- The observation key and metric constraint allow at most one row per series.
    HAVING COUNT(*) = 3
)
SELECT
    respondent,
    hour_start_utc,
    solar_mwh,
    wind_mwh,
    demand_mwh,
    solar_mwh + wind_mwh AS solar_wind_mwh,
    CASE WHEN demand_mwh > 0
        THEN (solar_mwh + wind_mwh) / demand_mwh
    END AS solar_wind_to_demand_ratio,
    demand_mwh - solar_mwh - wind_mwh AS demand_minus_solar_wind_mwh
FROM aligned;

CREATE OR REPLACE VIEW daily_energy_summary AS
WITH daily AS (
    SELECT
        respondent,
        (hour_start_utc AT TIME ZONE 'UTC')::date AS utc_date,
        COUNT(*) AS observed_hours,
        SUM(solar_mwh) AS solar_mwh,
        SUM(wind_mwh) AS wind_mwh,
        SUM(demand_mwh) AS demand_mwh,
        SUM(solar_wind_mwh) AS solar_wind_mwh,
        MAX(demand_mwh) AS peak_hourly_demand_mwh
    FROM hourly_energy_summary
    GROUP BY respondent, (hour_start_utc AT TIME ZONE 'UTC')::date
)
SELECT
    respondent,
    utc_date,
    observed_hours,
    solar_mwh,
    wind_mwh,
    demand_mwh,
    solar_wind_mwh,
    peak_hourly_demand_mwh,
    CASE WHEN demand_mwh > 0
        THEN solar_wind_mwh / demand_mwh
    END AS solar_wind_to_demand_ratio
FROM daily;
