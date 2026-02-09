-- Run Phase 1 in order
\i sql/01_raw.sql
\i sql/02_staging.sql
\i sql/03_dim_people_upsert.sql
\i sql/04_fact_events.sql
\i sql/05_quarantine_quality.sql
\i sql/06_indexes_explain.sql
