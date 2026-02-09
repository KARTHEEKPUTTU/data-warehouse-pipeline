-- Phase 1 Day 2: staging layer + cleaning rules
-- Purpose: clean raw_people into stg_people

drop table if exists stg_people;

create table stg_people(person_id INT PRIMARY KEY,full_name TEXT NOT NULL,age INT,email TEXT,country TEXT NOT NULL,load_ts TIMESTAMP DEFAULT NOW());

INSERT INTO stg_people(person_id,full_name,age,email,country) select person_id,TRIM(full_name) AS full_name,CASE WHEN age IS NULL THEN NULL WHEN age <0 OR age>120 THEN NULL ELSE age END AS age,NULLIF(TRIM(email),'')AS email,COALESCE (NULLIF(TRIM(country),''),'UNKNOWN') AS country from raw_people;

select count(*) as raw_count from raw_people;

select count(*) as stg_count from stg_people;

select count(*) filter(where email is null) as null_email,count(*) filter(where country = 'UNKNOWN') as unknown_country,count(*) filter(where age is null) as null_age from stg_people;

select * from stg_people order by person_id;