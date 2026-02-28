-- Daily Average Temperature Per City
SELECT d.location_name as location_name ,f.ts::date as date,AVG(f.temperature_2m) as Average_Temperature_city
FROM fact_weather_hourly f LEFT JOIN dim_location d ON f.location_id=d.location_id 
GROUP BY d.location_name,f.ts::date ORDER BY date desc;

-- Daily total precipitation per city
SELECT d.location_name as location_name,f.ts::date as date,sum(f.precipitation) as Total_Precipitation_city
FROM fact_weather_hourly f LEFT JOIN dim_location d ON f.location_id=d.location_id 
GROUP BY d.location_name,f.ts::date ORDER BY date desc;

-- Top 5 windiest hours per city
WITH ranked AS (
SELECT d.location_name as location_name,f.ts  as ts,f.windspeed_10m AS windspeed_10m,ROW_NUMBER() OVER (PARTITION BY d.location_name 
order by f.windspeed_10m DESC)  as rn
FROM fact_weather_hourly f LEFT JOIN dim_location d ON f.location_id=d.location_id 
)
SELECT * FROM ranked WHERE rn<=5 ORDER BY location_name,windspeed_10m DESC; 