drop table if exists raw_people;

-- create table
create table raw_people(person_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,full_name TEXT, age INT,email TEXT,country TEXT, created_at TIMESTAMP DEFAULT NOW());

-- Insert data into table
 INSERT INTO raw_people(full_name,age,email,country) VALUES('Kartheek Puttu',23,'kartheekp@gmail.com','India'),('
Aakash Chous',NULL,'aakashch@gmail.com','USA'),('Vibe Coder',26,NULL,'UK'),('Tester',43,'tester@gmail.com','USA'),('Amer
icano San',45,'americsan@gmail.com','UK');

-- NUll / Range / duplicate checks
select COUNT(*) as total_rows ,count(*) FILTER (WHERE email IS NULL) AS null_email,COUNT(*) FILTER (WHERE age IS
NULL) AS null_age,COUNT(*) FILTER (WHERE country IS NULL) AS null_country FROM raw_people;

SELECT * FROM raw_people WHERE age IS NOT NULL AND (age < 0 OR age > 120);

select email,count(*) as cnt_email from raw_people where email is not null group by email having count(*) >1;

