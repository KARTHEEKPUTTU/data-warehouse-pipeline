-- Day 6 Phase 1 Day 6: Indexes + EXPLAIN (query plans)
-- How postgres executes joins/filters and also when indexes are going to be used..

CREATE INDEX IF NOT EXISTS idx_dim_people_email ON dim_people(email);
CREATE INDEX IF NOT EXISTS idx_fact_events_email ON fact_events(email);
CREATE INDEX IF NOT EXISTS idx_fact_events_event_type ON fact_events(event_type);

-- EXPLAIN (normal planner choice; may still show Seq Scan due to small tables)
EXPLAIN SELECT d.country,SUM(f.amount) AS total_revenue_per_country FROM fact_events f JOIN dim_people d ON f.email = d.email
WHERE event_type = 'purchase' group by d.country order by total_revenue_per_country DESC;

-- EXPLAIN with index-forcing (LEARNING ONLY)
-- enable_seqscan=off is not for production; it helps you see an index plan.
SET enable_seqscan = off;

EXPLAIN
SELECT
  d.country,
  SUM(f.amount) AS total_revenue_per_country
FROM fact_events f
JOIN dim_people d ON f.email = d.email
WHERE f.event_type = 'purchase'
GROUP BY d.country
ORDER BY total_revenue_per_country DESC;

EXPLAIN ANALYZE
SELECT
  d.country,
  SUM(f.amount) AS total_revenue_per_country
FROM fact_events f
JOIN dim_people d ON f.email = d.email
WHERE f.event_type = 'purchase'
GROUP BY d.country
ORDER BY total_revenue_per_country DESC;

RESET enable_seqscan;

-- Optional: show created indexes (psql meta commands)
-- \di dim_people*
-- \di fact_events*
-- \d dim_people
-- \d fact_events