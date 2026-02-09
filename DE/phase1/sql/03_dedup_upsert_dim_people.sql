-- Phase 1 Day 3: Curated layer (dim_people) with dedup + upsert
-- Business key: email
-- Latest record chosen by: raw_people.created_at (desc), then person_id (desc)

DROP TABLE IF EXISTS dim_people;

CREATE TABLE dim_people (
  email TEXT PRIMARY KEY,
  full_name TEXT NOT NULL,
  age INT,
  country TEXT NOT NULL,
  source_person_id INT NOT NULL,
  source_created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Upsert latest-per-email from staging + raw timestamps
INSERT INTO dim_people (email, full_name, age, country, source_person_id, source_created_at, updated_at)
SELECT
  email,
  full_name,
  age,
  country,
  person_id AS source_person_id,
  created_at AS source_created_at,
  NOW() AS updated_at
FROM (
  SELECT
    r.person_id,
    s.full_name,
    s.age,
    s.email,
    s.country,
    r.created_at,
    ROW_NUMBER() OVER (
      PARTITION BY s.email
      ORDER BY r.created_at DESC, r.person_id DESC
    ) AS rn
  FROM stg_people s
  JOIN raw_people r
    ON r.person_id = s.person_id
  WHERE s.email IS NOT NULL
) x
WHERE rn = 1
ON CONFLICT (email) DO UPDATE
SET
  full_name = EXCLUDED.full_name,
  age = EXCLUDED.age,
  country = EXCLUDED.country,
  source_person_id = EXCLUDED.source_person_id,
  source_created_at = EXCLUDED.source_created_at,
  updated_at = EXCLUDED.updated_at;

-- Adding duplicate record..to check 
-- INSERT INTO raw_people(full_name,age,email,country) VALUES('Kartheek Puttu',23,'kartheekp@gmail.com','India');

-- TRUNCATE table stg_people;

-- INSERT INTO stg_people(person_id,full_name,age,email,country) SELECT person_id,TRIM(full_name) AS full_name,CASE WHEN age IS NULL THEN NULL WHEN age
--  <0 and age>120 THEN NULL ELSE age END AS age,NULLIF(TRIM(email),'') AS email,COALESCE(NULLIF(TRIM(country),''),'UNKNOWN') AS country FROM raw_people;

-- insert into dim_people(email,full_name,age,country,source_person_id,source_created_at,updated_at) select email,full_name,age,country,person_id AS source_person_id,
-- created_at AS source_created_at,NOW() AS updated_at FROM (SELECT r.person_id,s.full_name,s.age,s.email,s.country,r.created_at,
-- ROW_NUMBER() OVER (partition by s.email order by r.created_at desc, r.person_id desc) AS rn 
-- FROM stg_people s JOIN raw_people r ON r.person_id = s.person_id where s.email is not null)x 
-- where rn =1 ON CONFLICT(email) DO UPDATE SET full_name = EXCLUDED.full_name,age = EXCLUDED.age,
-- country = EXCLUDED.country,source_person_id = EXCLUDED.source_person_id,source_created_at = EXCLUDED.source_created_at,
-- updated_at = EXCLUDED.updated_at;

-- Verification
-- SELECT COUNT(*) AS dim_count FROM dim_people;
-- SELECT * FROM dim_people ORDER BY email;
