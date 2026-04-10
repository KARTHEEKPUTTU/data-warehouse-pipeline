# Open-Meteo Weather Data Pipeline (Postgres + Airflow + Snowflake)
An end-to-end data engineering pipeline that ingests hourly weather data from **Open-Meteo** into **PostgreSQL** with
**idempotent upserts**, **watermark-based incremental loading**, **audit logging**, and **quality checks**.
Orchestrated with **Apache Airflow (Docker)**, supports **multi-city parallel ingestion**, and exports to **AWS S3** and **Snowflake** for cloud-based analytics.

## Project Phases:
| Phase | Folder | What it covers |
|-------|--------|----------------|
| 1 | [`DE/phase1`](DE/phase1) | SQL warehouse basics: raw → staging → curated, rerunnable scripts, data-quality checks, star schema intro |
| 2 | [`DE/phase2`](DE/phase2) | Initial Open-Meteo ingestion into Postgres raw tables + audit table + Windows Task Scheduler automation |
| 3 | [`DE/phase3`](DE/phase3) | Incremental loader (watermark): idempotency, `ingestion_runs` audit, QC + rollback on failure |
| 4 | [`DE/phase4_airflow`](DE/phase4_airflow) | Airflow orchestration in Docker: schedule, retries/timeouts, QC task, multi-city ingestion |
| 5 | [`DE/phase5_modeling`](DE/phase5_modeling) | Modeling in Postgres: STG view, dim_date, fact_weather_hourly, QC + analytics queries |
| 6 | [`DE/phase6_cloud_elt`](DE/phase6_cloud_elt) | Cloud export: Postgres → AWS S3 as partitioned CSV (Hive-style layout by date + city) |
| 7 | [`DE/phase7_snowflake`](DE/phase7_snowflake) | Snowflake load: S3 → Snowflake via external stage + COPY INTO |

---

## End-to-end Architecture
Open-Meteo API
↓
Phase 3/4 — Incremental load into Postgres (Airflow orchestrated)
↓
Phase 5 — Modeling: STG → dim_date → fact_weather_hourly
↓
Phase 6 — Export Postgres → AWS S3 (partitioned CSV)
↓
Phase 7 — Load S3 → Snowflake (COPY INTO via external stage)

---

## Quick Start:

```bash
# Start (or resume) the Postgres container used for the warehouse
docker start de-postgres
```

**Phase 1 (SQL-only warehouse build):**
```bash
# Windows CMD
DE\phase1\run_phase1.cmd
```

**Phase 2 (one-time ingestion):**
```bash
python DE/phase2/scripts/load_openmeteo_hourly.py --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```

**Phase 3 (incremental hourly ingestion):**
```bash
python DE/phase3/scripts/load_openmeteo_hourly_incremental.py --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```

**Phase 4 (Airflow orchestration):**
```bash
cd DE/phase4_airflow
# Copy .env.example -> .env and fill in your local values (do not commit .env)
docker compose up -d --build
# Airflow UI: http://localhost:8080
```

**Phase 5 (modeling in Postgres):**
```bash
# See DE/phase5_modeling/README.md for the run order
# STG → dim_date → fact → load → QC
```

**Phase 6 (export to S3):**
```bash
python DE/phase6_cloud_elt/scripts/export_to_s3.py
```

**Phase 7 (load into Snowflake):**
```bash
python DE/phase7_snowflake/scripts/load_to_snowflake.py
```

---

Full setup instructions, architecture, and troubleshooting are documented in each phase folder README.
