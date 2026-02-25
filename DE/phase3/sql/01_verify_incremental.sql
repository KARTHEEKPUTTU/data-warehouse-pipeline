-- Recent runs
SELECT run_id, status, row_count, started_at, finished_at, error_message
FROM ingestion_runs
WHERE pipeline_name = 'Phase3_openmeteo_hourly_incremental'
ORDER BY run_id DESC
LIMIT 10;

-- Row growth
SELECT COUNT(*) FROM raw_weather_hourly;

-- Watermark per location
SELECT location_id, MAX(ts) AS max_ts
FROM raw_weather_hourly
GROUP BY 1
ORDER BY 1;

-- Duplicate check (should return 0 rows)
SELECT location_id, ts, COUNT(*) AS cnt
FROM raw_weather_hourly
GROUP BY 1, 2
HAVING COUNT(*) > 1;