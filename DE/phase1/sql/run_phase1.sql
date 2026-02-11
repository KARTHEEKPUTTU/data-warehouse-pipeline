-- Run Phase 1 in order
\i 01_raw.sql
\i 02_staging.sql
\i 03_dim_people_upsert.sql
\i 04_fact_events.sql
\i 05_quarantine_quality.sql
\i 06_indexes_explain.sql
