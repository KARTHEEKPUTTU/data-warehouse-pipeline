CREATE OR REPLACE VIEW stg_weather_hourly AS
SELECT r.location_id,r.ts,r.temperature_2m,r.precipitation,r.windspeed_10m,
r.source,r.ingested_at,r.run_id,d.location_name 
FROM raw_weather_hourly r JOIN dim_location d ON r.location_id = d.location_id; 