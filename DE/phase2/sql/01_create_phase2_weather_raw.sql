CREATE TABLE IF NOT EXISTS ingestion_runs(
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    params JSONB NOT NULL,
    started_at TIMESTAMP DEFAULT NOW(),
    finished_at TIMESTAMP,
    status TEXT DEFAULT 'RUNNING',
    row_count BIGINT DEFAULT 0,
    error_message TEXT 
);

CREATE TABLE IF NOT EXISTS dim_location(
    location_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_name TEXT NOT NULL,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    UNIQUE(latitude,longitude)
);

CREATE TABLE IF NOT EXISTS raw_weather_hourly(
    location_id BIGINT NOT NULL REFERENCES dim_location(location_id),
    ts TIMESTAMP NOT NULL,
    temperature_2m NUMERIC,
    precipitation NUMERIC,
    windspeed_10m NUMERIC,
    source TEXT DEFAULT 'open-meteo',
    ingested_at TIMESTAMP DEFAULT NOW(),
    run_id BIGINT NOT NULL REFERENCES ingestion_runs(run_id),
    PRIMARY KEY(location_id,ts)
);