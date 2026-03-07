## Phase 5 — Modeling (RAW → STG → CURATED) in Postgres
- Phase 5 takes the raw Open-Meteo ingestion tables from Phase 3/4 and turns them into analytics-ready tables using a simple warehouse modeling approach.

# Goal
- Keep the raw ingestion tables unchanged for traceability
- Create a clean staging layer for consistent downstream use
- Model curated fact and dimension tables for analytics queries
- Validate correctness with basic quality checks

# Data model and grain
- Grain: one row per location per hour
- The fact table keeps the same grain as the raw data and enforces uniqueness.

# Objects created
- stg_weather_hourly (VIEW)
    A clean staging view that joins raw hourly weather with location context like location_name.
- dim_date (TABLE)
    Date dimension built from distinct dates present in the ingested data. Includes a deterministic date_key in YYYYMMDD format.
- fact_weather_hourly (TABLE)
    Curated hourly fact table for analytics. Uses keys and constraints:
    - Primary key: location_id, ts
    - Foreign keys: location_id → dim_location, date_key → dim_date, run_id → ingestion_runs

# How to run
Run the SQL scripts in this order:
  01_stg_weather_hourly.sql
  02_dim_date.sql
  03_fact_weather_hourly.sql
  04_load_fact_weather_hourly.sql
  05_quality_checks.sql
  06_analytics_queries.sql (optional, for sample analysis)

# Quality checks
Phase 5 includes basic QC queries to confirm:
- staging row count matches fact row count
- no missing joins to dim_date or dim_location
- no duplicate pairs of location_id, ts in the fact table

In my runs, the checks returned:
- row counts match
- missing dates = 0
- missing locations = 0
- duplicates = none

# Result
After Phase 5, the pipeline produces a clean, reusable analytics layer in Postgres that can be queried directly or extended later with additional dimensions, metrics, and reporting.
