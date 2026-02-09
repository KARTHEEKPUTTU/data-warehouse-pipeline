# Phase 1 – Postgres Data Warehouse Mini Pipeline (Docker)

This is my Phase 1 data engineering practice project.  
Goal: build a small but realistic pipeline in PostgreSQL (running in Docker) using SQL only.

I created a simple “raw → staging → curated” flow, added data quality checks, and verified results with analytics queries + EXPLAIN plans.

---

## What I built (high level)

### 1) Raw layer (landing/audit)
- **raw_people**: user/person data as received (can include blanks/nulls)
- **raw_events**: event data (login/purchase). Also can include bad rows.

Raw tables are like “first stop” tables. I keep data close to the source so I can debug issues later.

### 2) Staging layer (cleaning)
- **stg_people**
- Cleaning logic includes:
  - trim spaces
  - convert empty strings to NULL
  - handle missing country (set to `UNKNOWN`)
  - age validation (remove invalid ranges)

This is where I standardize data before using it downstream.

### 3) Curated layer (trusted tables for analytics)
- **dim_people** (dimension table)
  - one row per person using **email** as the business key
  - dedup logic: keep the **latest** record per email
  - uses `ROW_NUMBER()` + `ON CONFLICT DO UPDATE` (upsert)
  - stores lineage fields like `source_person_id`, `source_created_at`

- **fact_events** (fact table)
  - stores events like login/purchase with timestamp and amount
  - enriched with `is_known_person` flag using a join to `dim_people`

### 4) Data quality (quarantine pattern)
- **quarantine_events**
  - rows that fail rules go here with a `reject_reason`
  - example: missing email, purchase with missing amount, negative amount

Also added table constraints (CHECK rules) so bad data can’t enter curated tables by mistake.

### 5) Performance basics
- created indexes on join/filter columns
- used `EXPLAIN` and `EXPLAIN ANALYZE` to understand query plans
- learned how planner can choose seq scan vs index scan (especially on small tables)

---

## Why I chose this design
This is a common pattern I see in real DE discussions:
- Raw tables keep “what came in”
- Staging cleans data
- Curated tables are stable for BI/reporting
- Quarantine helps investigate bad records instead of deleting them

---

## How to run (manual)
Start Postgres container and connect:

```bash
docker exec -it de-postgres psql -U de_user -d de_db
```

**## Then run scripts in order from inside psql:**
\i DE/phase1/sql/01_create_insert_validate.sql
\i DE/phase1/sql/02_staging_clean_load.sql
\i DE/phase1/sql/03_dedup_upsert_dim_people.sql
\i DE/phase1/sql/04_fact_events_star_schema.sql
\i DE/phase1/sql/05_data_quality_quarantine.sql
\i DE/phase1/sql/06_indexes_explain.sql

**## Files:**
```
DE/phase1/sql/
  01_create_insert_validate.sql
  02_staging_clean_load.sql
  03_dedup_upsert_dim_people.sql
  04_fact_events_star_schema.sql
  05_data_quality_quarantine.sql
  06_indexes_explain.sql
  run_phase1.sql
```
**## Example checks I did**
- raw_events count vs fact_events + quarantine_events count
- revenue by country and purchase per person using fact + dim join
- confirmed dedup: same email in raw_people updates the row in dim_people (latest record wins)

**## What I’ll improve next (Phase 2+)**
- load raw tables from CSV/JSON using Python
- automate running SQL scripts cleanly
- more realistic schema + larger data volumes
- add tests/validation reports
