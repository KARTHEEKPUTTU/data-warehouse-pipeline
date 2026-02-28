CREATE TABLE IF NOT EXISTS fact_weather_hourly(
    location_id BIGINT NOT NULL REFERENCES dim_location(location_id),
    ts TIMESTAMP NOT NULL,
    temperature_2m numeric, precipitation numeric, windspeed_10m numeric, 
    date_key INT NOT NULL REFERENCES dim_date(date_key), 
    run_id BIGINT NOT NULL REFERENCES ingestion_runs(run_id), 
    PRIMARY KEY(location_id,ts)
);