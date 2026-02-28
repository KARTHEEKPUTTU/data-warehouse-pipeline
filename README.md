# Open-Meteo Weather Data Pipeline (Postgres + Airflow)

An end-to-end data engineering pipeline that ingests hourly weather data from **Open-Meteo** into **PostgreSQL** with
**idempotent upserts**, **watermark-based incremental loading**, **audit logging**, and **quality checks**.
Orchestrated with **Apache Airflow (Docker)** and supports **multi-city parallel ingestion**.

## Project Phases

| Phase |          Folder          |                                               What it covers                                              |
|------ |--------------------------|-----------------------------------------------------------------------------------------------------------|
| 1     | [`DE/phase1`](DE/phase1) | SQL warehouse basics: raw → staging → curated, rerunnable scripts, data-quality checks, star schema intro |
| 2     | [`DE/phase2`](DE/phase2) | Initial Open-Meteo ingestion into Postgres raw tables + audit table + Windows Task Scheduler automation |
| 3     | [`DE/phase3`](DE/phase3) | Incremental loader (watermark): idempotency, `ingestion_runs` audit, QC + rollback on failure |
| 4     | [`DE/phase4_airflow`](DE/phase4_airflow) | Airflow orchestration in Docker: schedule, retries/timeouts, QC task, multi-city ingestion |
| 5     | [\DE/phase5_modeling`](DE/phase5_modeling) | Modeling in Postgres: STG view, dim_date, fact_weather_hourly, QC + analytics queries |`
---

## Quick Start

```bash
# Start (or resume) the Postgres container used for the warehouse
docker start de-postgres
```
Phase 1 (SQL-only warehouse build)
```
# Windows CMD
DE\phase1\run_phase1.cmd
```

Phase 2 (one-time ingestion)
```
python DE/phase2/scripts/load_openmeteo_hourly.py --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```

Phase 3 (incremental hourly ingestion)
```
python DE/phase3/scripts/load_openmeteo_hourly_incremental.py --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```

Phase 4 (Airflow orchestration)
```
cd DE/phase4_airflow
# Copy .env.example -> .env and fill in your local values (do not commit .env)
docker compose up -d --build
# Airflow UI: http://localhost:8080
```
Phase 5 (modeling in Postgres)
See DE/phase5_modeling/README.md for the run order (STG → dim_date → fact → load → QC).

Full setup instructions, architecture, and troubleshooting are documented in each phase folder README.
