INSERT INTO fact_weather_hourly(location_id,ts,temperature_2m,precipitation,windspeed_10m,date_key,run_id) 
SELECT s.location_id,s.ts,s.temperature_2m,s.precipitation,s.windspeed_10m,dk.date_key,s.run_id FROM stg_weather_hourly s 
JOIN dim_date dk ON dk.date=s.ts::date ON CONFLICT (location_id, ts) 
DO UPDATE SET temperature_2m=EXCLUDED.temperature_2m,
precipitation=EXCLUDED.precipitation,windspeed_10m=EXCLUDED.windspeed_10m,
date_key=EXCLUDED.date_key,run_id=EXCLUDED.run_id;