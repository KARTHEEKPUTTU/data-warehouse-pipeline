-- Row Count Reconcillation
select (select count(*) from stg_weather_hourly) AS row_count_stg,
(select count(*) from fact_weather_hourly) AS row_count_fact;

-- Missing Dates
select count(*) AS missing_dates from fact_weather_hourly f 
LEFT JOIN dim_date d ON f.date_key=d.date_key
WHERE d.date_key IS NULL;

-- Missing Location
select count(*) AS missing_location from fact_weather_hourly f
LEFT JOIN dim_location l ON f.location_id=l.location_id
WHERE l.location_id IS NULL;

-- Duplicate grain check
select location_id,ts,count(*) AS Duplicate_count from fact_weather_hourly group by location_id,ts HAVING COUNT(*)>1;