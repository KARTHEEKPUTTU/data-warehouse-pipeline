# Phase 3 — Incremental Open-Meteo Loader (Python → Postgres)

Phase 3 turns the Phase 2 one-time ingestion into a pipeline-style **incremental loader**.

What this phase focuses on:
- **Incremental loading** using a **watermark** (load only new hours)
- **Idempotency** using Postgres **upserts** (safe to re-run)
- **Auditability** via an `ingestion_runs` table (what ran, when, status, row count, errors)
- **Quality checks** with **transaction rollback** on failure

---

## What’s in this folder

```
DE/phase3/
scripts/
load_openmeteo_hourly_incremental.py
run_openmeteo_incremental.cmd
sql/
01_verify_incremental.sql
logs/
openmeteo_task_incremental.log
README.md
```

> `logs/` is just local execution output. It shouldn’t be committed to git.

---

## Tables used

### `dim_location`
Stores a unique location (name + latitude/longitude).

- Uniqueness: `(latitude, longitude)`
- Loader behavior: **upsert** and reuse the same `location_id`

### `raw_weather_hourly`
Hourly weather facts.

- Primary key: `(location_id, ts)` → prevents duplicates at the correct grain
- Loader behavior: **upsert** hourly rows (updates the measures if the row already exists)

### `ingestion_runs`
Run-level audit log.

Tracks:
- pipeline name + params (stored as JSON)
- started/finished timestamps
- status: `RUNNING` / `SUCCESS` / `FAILED` / `SKIPPED`
- row_count + error_message

---

## How the incremental load works

At a high level, each run does this:

1) **Upsert** the location into `dim_location` and get `location_id`  
2) Read the **watermark**:
   - `SELECT MAX(ts) FROM raw_weather_hourly WHERE location_id = ?`
3) Create an audit record in `ingestion_runs` (status starts as `RUNNING`)
4) Call the Open-Meteo API for hourly data (`temperature_2m`, `precipitation`, `windspeed_10m`)
5) **Upsert** the hourly rows into `raw_weather_hourly`
6) Run **QC checks**
7) Mark run `SUCCESS` / `FAILED` / `SKIPPED`

### Watermark behavior
- **First run**: loads the default forecast window (`today … today+6`)
- **Later runs**: loads from `last_ts + 1 hour` up to `today+6`

### Forecast horizon guard (`SKIPPED`)
Open-Meteo forecast has a limited horizon. If the watermark is already beyond the current horizon:
- no new hours exist to ingest
- the run is marked **SKIPPED**
- the script exits cleanly (no false failure)

---

## How to run

### Run via Python
From repo root:

```bash
python DE/phase3/scripts/load_openmeteo_hourly_incremental.py --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```

# Run via Windows wrapper (writes logs)
```
DE\phase3\run_openmeteo_incremental.cmd
```
# Configuration (current):

- Right now the loader uses a hard-coded Postgres connection in get_conn():
- host = host.docker.internal
- port = 5432
- dbname = de_db
- user = de_user
- password = postgres

This works for local execution and for running from containers (Phase 4).

(Improvement I’ll do later: read these from environment variables so the script is portable.)

# Quality checks + transaction safety

This loader runs inside an explicit transaction (autocommit = False).

QC is intentionally strict:

- fail if rows_touched == 0
- fail if any NULLs exist in the temperature list (guardrail)

On QC failure:

- data changes are rolled back
- ingestion_runs is updated to FAILED 
- the audit update is committed
- the script raises an error

On success:

- ingestion_runs is updated to SUCCESS with row_count

## Quick verification SQL
Recent runs
```
SELECT run_id, status, row_count, started_at, finished_at, error_message
FROM ingestion_runs
WHERE pipeline_name = 'Phase3_openmeteo_hourly_incremental'
ORDER BY run_id DESC
LIMIT 10;
```
Row growth
```
SELECT COUNT(*) FROM raw_weather_hourly;
Watermark per location
SELECT location_id, MAX(ts) AS max_ts
FROM raw_weather_hourly
GROUP BY 1
ORDER BY 1;
```
Duplicate check (should return 0 rows)
```
SELECT location_id, ts, COUNT(*) AS cnt
FROM raw_weather_hourly
GROUP BY 1, 2
HAVING COUNT(*) > 1;
```
Expected outcomes (what I observed)

- First run per location typically inserts 168 rows (7 days × 24 hours)

Later runs may:

- insert more rows as the forecast window advances (ex: +72)

- mark the run as SKIPPED if the watermark is beyond the forecast window

# Notes from development

During development I used the .cmd wrapper + log file to capture errors quickly (argparse typos, missing packages, invalid date ranges, etc.). The final script includes the main guardrails I needed:

- correct start/end date logic

- SKIPPED status when no new hours exist

- transactional rollback on QC failure

# Why Phase 3 matters

This phase is where the project starts to look like a real pipeline:

- watermark-based incremental loads
- idempotent upserts
- run audit history
- QC checks with rollback instead of “partial loads”