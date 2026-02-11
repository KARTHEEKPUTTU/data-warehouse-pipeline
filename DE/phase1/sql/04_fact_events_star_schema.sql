-- Phase 1 Day 4: Fact table + Star Schema joins
-- Goal: raw_events -> fact_events, then analyze with dim_people

-- 1) RAW events (source landing)
DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events (
  event_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_ts TIMESTAMP NOT NULL,
  email TEXT,
  event_type TEXT NOT NULL,
  amount NUMERIC(10,2),
  source TEXT DEFAULT 'web',
  created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO raw_events (event_ts, email, event_type, amount, source) VALUES
('2026-02-06 10:00:00', 'kartheekp@gmail.com', 'login', NULL, 'web'),
('2026-02-06 10:05:00', 'tester@gmail.com',   'purchase', 29.99, 'web'),
('2026-02-06 10:07:00', 'americsan@gmail.com','purchase', 199.00, 'store'),
('2026-02-06 10:10:00', NULL,                 'login', NULL, 'web'),
('2026-02-06 10:12:00', 'unknown@gmail.com',  'purchase', 10.00, 'web');

-- 2) FACT events (curated)
DROP TABLE IF EXISTS fact_events;

CREATE TABLE fact_events (
  event_id INT PRIMARY KEY,
  event_ts TIMESTAMP NOT NULL,
  email TEXT,
  event_type TEXT NOT NULL,
  amount NUMERIC(10,2),
  source TEXT,
  is_known_person BOOLEAN NOT NULL,
  load_ts TIMESTAMP DEFAULT NOW(),

  CONSTRAINT chk_event_type_not_blank CHECK (TRIM(event_type) <> ''),
  CONSTRAINT chk_purchase_amount CHECK (event_type <> 'purchase' OR amount IS NOT NULL),
  CONSTRAINT chk_amount_non_negative CHECK (amount IS NULL OR amount >= 0)
);

-- Load fact from raw with dim lookup
INSERT INTO fact_events (event_id, event_ts, email, event_type, amount, source, is_known_person)
SELECT
  r.event_id,
  r.event_ts,
  r.email,
  r.event_type,
  r.amount,
  r.source,
  (d.email IS NOT NULL) AS is_known_person
FROM raw_events r
LEFT JOIN dim_people d
  ON d.email = r.email;

-- 3) Validations
SELECT COUNT(*) AS raw_events_count FROM raw_events;
SELECT COUNT(*) AS fact_events_count FROM fact_events;

SELECT
  COUNT(*) FILTER (WHERE email IS NULL) AS null_email_events,
  COUNT(*) FILTER (WHERE is_known_person = false) AS unknown_person_events
FROM fact_events;

-- 4) Analytics queries

-- Revenue by country (known persons only)
SELECT
  d.country,
  SUM(f.amount) AS total_revenue
FROM fact_events f
JOIN dim_people d ON d.email = f.email
WHERE f.event_type = 'purchase'
GROUP BY d.country
ORDER BY total_revenue DESC;

-- Purchases per person (include 0 purchases)
SELECT
  d.email,
  d.full_name,
  COUNT(*) FILTER (WHERE f.event_type = 'purchase') AS purchase_count,
  COALESCE(SUM(f.amount) FILTER (WHERE f.event_type = 'purchase'), 0) AS purchase_revenue
FROM dim_people d
LEFT JOIN fact_events f
  ON f.email = d.email
GROUP BY d.email, d.full_name
ORDER BY purchase_revenue DESC;
