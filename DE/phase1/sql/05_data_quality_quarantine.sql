-- Phase 1 Day 5: Data Quality Gates + Quarantine Pattern
-- Goal: split raw_events into good (fact_events) and bad (quarantine_events)

-- 1) Quarantine table
DROP TABLE IF EXISTS quarantine_events;

CREATE TABLE quarantine_events (
  quarantine_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id INT,
  event_ts TIMESTAMP,
  email TEXT,
  event_type TEXT,
  amount NUMERIC(10,2),
  source TEXT,
  reject_reason TEXT NOT NULL,
quarantined_at TIMESTAMP DEFAULT NOW()
);

-- 3) Removing the alredy existing data ..
TRUNCATE TABLE fact_events;
TRUNCATE TABLE quarantine_events;

-- 4) Sending Bad rows to Quarantine_events
INSERT INTO quarantine_events (event_id, event_ts, email, event_type, amount, source, reject_reason)
SELECT
  event_id,
  event_ts,
  email,
  event_type,
  amount,
  source,
  CASE
    WHEN email IS NULL THEN 'missing_email'
    WHEN event_type = 'purchase' AND amount IS NULL THEN 'purchase_missing_amount'
    WHEN amount < 0 THEN 'negative_amount'
    ELSE 'other'
  END AS reject_reason
FROM raw_events
WHERE
  email IS NULL
  OR (event_type = 'purchase' AND amount IS NULL)
  OR (amount < 0);

-- 5) sending good rows to fact_events
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
LEFT JOIN dim_people d ON d.email = r.email
WHERE
  r.email IS NOT NULL
  AND NOT (r.event_type = 'purchase' AND r.amount IS NULL)
  AND (r.amount IS NULL OR r.amount >= 0);

-- 4) Verifications
SELECT COUNT(*) AS raw_events_count FROM raw_events;
SELECT COUNT(*) AS fact_events_count FROM fact_events;
SELECT COUNT(*) AS quarantined_count FROM quarantine_events;

SELECT reject_reason, COUNT(*) AS rejected_count
FROM quarantine_events
GROUP BY reject_reason
ORDER BY rejected_count DESC;

SELECT * FROM quarantine_events ORDER BY quarantine_id;
SELECT * FROM fact_events ORDER BY event_id;