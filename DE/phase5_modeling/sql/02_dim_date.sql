create table IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    date Date NOT NULL UNIQUE
);
INSERT INTO dim_date(date_key,date)
SELECT DISTINCT to_char(ts::date,'YYYYMMDD')::INT,ts::date 
FROM stg_weather_hourly ON CONFLICT(date) DO NOTHING; 

