WITH seasonal_hours AS (
    SELECT
        h.respondent,
        d.utc_date,
        CASE
            WHEN EXTRACT(MONTH FROM d.utc_date) IN (12, 1, 2) THEN 'winter'
            WHEN EXTRACT(MONTH FROM d.utc_date) IN (3, 4, 5) THEN 'spring'
            WHEN EXTRACT(MONTH FROM d.utc_date) IN (6, 7, 8) THEN 'summer'
            ELSE 'autumn'
        END AS season,
        EXTRACT(HOUR FROM h.hour_start_utc AT TIME ZONE 'UTC')::integer AS hour_utc,
        h.solar_mwh,
        h.wind_mwh,
        h.demand_mwh
    FROM hourly_energy_summary AS h
    JOIN daily_energy_summary AS d
      ON d.respondent = h.respondent
     AND d.utc_date = (h.hour_start_utc AT TIME ZONE 'UTC')::date
    WHERE d.observed_hours = 24
)
SELECT
    respondent,
    season,
    hour_utc,
    COUNT(*) AS observed_days,
    MIN(utc_date) AS first_utc_date,
    MAX(utc_date) AS last_utc_date,
    AVG(solar_mwh) AS mean_solar_mwh,
    AVG(wind_mwh) AS mean_wind_mwh,
    AVG(demand_mwh) AS mean_demand_mwh
FROM seasonal_hours
GROUP BY respondent, season, hour_utc
ORDER BY respondent, season, hour_utc;
