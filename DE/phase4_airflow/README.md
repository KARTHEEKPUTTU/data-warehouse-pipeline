# Phase 4 — Airflow Orchestration (Docker)

Phase 4 replaces the Phase 3 Windows Task Scheduler approach with **Apache Airflow** running in Docker.

The goal here is to run the Phase 3 incremental loader as a scheduled, reliable workflow with:
- scheduling + retries + timeouts
- centralized logs in Airflow UI
- a QC step that validates the latest run
- multi-city ingestion running in parallel

---

## What’s in this folder

DE/phase4_airflow/
dags/
openmeteo_incremental.py
qc_openmeteo.py
docker-compose.yaml
Dockerfile
.env.example
README.md

> `.env` is local-only and should not be committed. Same for `__pycache__/` and `*.pyc`.

---

## Architecture (high level)

**Airflow (Docker)** orchestrates the ingestion, but the actual ingestion logic remains in Phase 3.

- Airflow runs a DAG using **BashOperator**
- Each task executes the Phase 3 loader:
  - `python /opt/airflow/phase3/scripts/load_openmeteo_hourly_incremental.py ...`
- After ingestion tasks finish, Airflow runs a separate QC script:
  - `python /opt/airflow/dags/qc_openmeteo.py`

**Key mounts**
- `./dags → /opt/airflow/dags`
- `../phase3 → /opt/airflow/phase3`

So Airflow containers can directly run the Phase 3 loader without copying code.

---

## Services in Docker Compose

`docker-compose.yaml` brings up:
- `postgres-airflow` — Airflow metadata database (Postgres 15)
- `redis` — Celery broker
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker`
- `airflow-triggerer`

Optional (included in my compose):
- `de-postgres` — a Postgres 16 container that hosts the project tables (`dim_location`, `raw_weather_hourly`, `ingestion_runs`)

---

## Setup / Configuration

### 1) Create `.env`
Copy the template:

- Copy `.env.example` → `.env`
- Fill in values locally (do not commit `.env`)

`.env.example` contains:
- Airflow DB + Celery settings
- DE Postgres connection details (`DE_DB_HOST`, `DE_DB_PORT`, etc.)

### 2) Build the Airflow image
This repo uses a custom image to include Celery dependencies.

From `DE/phase4_airflow/`:

```bash
docker build -t kartheek-airflow:2.9.0-celery .
```
3) Start the stack
```
docker compose up -d
```
Airflow UI:
```
http://localhost:8080
```
# Important: DE Postgres connection (containers vs host)

- Airflow runs inside containers, so DB host/port matters.

You have two valid ways to connect to the de_db database:

Option A (recommended): Use the de-postgres service inside compose

- If you are using the de-postgres service defined in docker-compose.yaml, then inside Airflow:
- DE_DB_HOST=de-postgres
- DE_DB_PORT=5432
- This is the cleanest “container-to-container” networking.

Option B: Connect to Postgres on the host using host.docker.internal

- If your de_db is running outside this compose stack (or you want to reach host ports), then use:
- DE_DB_HOST=host.docker.internal
- DE_DB_PORT=<host port>

⚠️ In your compose, de-postgres is mapped as:

"5433:5432"

So if you go through host networking, the correct port is usually:

DE_DB_PORT=5433

## DAG: openmeteo_incremental

# File: dags/openmeteo_incremental.py

What it does:

- Runs Phase 3 incremental loader for multiple cities in parallel:

    - Chicago
    - New York
    - Los Angeles

- Then runs the QC task

# Scheduling and timezone

- DAG timezone: America/Chicago

- Example schedule in the DAG: 00 4 * * * (4:00 AM Chicago)

Note: Airflow UI may show next run in UTC even though the DAG is configured with Chicago timezone. The cron schedule still follows the DAG timezone.

# Retries / reliability

Each city task has:

- retries=2
- retry_delay=2 minutes
- execution_timeout=10 minutes

So transient issues show as UP_FOR_RETRY and then succeed later.

QC step: qc_openmeteo.py

File: dags/qc_openmeteo.py

QC logic:

1. Uses QC_RUN_TS (Airflow {{ ts }}) to search a small time window for the latest ingestion_runs row
2. Allows status: SUCCESS or SKIPPED
3. If SUCCESS, enforces:

- finished_at is not null

- row_count > 0

- no NULLs in raw_weather_hourly fields for that run_id

- no duplicates for (location_id, ts) for that run_id

This keeps “bad loads” from silently passing.

How to run (end-to-end)

1. Start containers:
```
docker compose up -d
```
2. Open Airflow UI:
```
http://localhost:8080
```
3. Enable the DAG:

openmeteo_incremental

4. Trigger manually for testing:

Click Trigger DAG

5. Verify in Postgres:
```
SELECT run_id, status, row_count
FROM ingestion_runs
ORDER BY run_id DESC
LIMIT 10;

SELECT COUNT(*) FROM raw_weather_hourly;
```
## Common issues I hit (and how I fixed them)
# Connection refused to localhost:5432

- Inside containers, localhost means the container itself.
- Fix: use de-postgres (container DNS) or host.docker.internal (host access).

# Airflow shows 403 / secret key mismatch

- Fix: ensure the same AIRFLOW__WEBSERVER__SECRET_KEY is used for all services.

# UI shows 400 after changes

- Usually stale cookies.
- Fix: clear site data or use Incognito.

# Import errors / wrong operator imports

- Use correct imports (ex: from airflow.operators.bash import BashOperator).

# Notes for repo hygiene (recommended)

Make sure these are ignored in git:

- .env
- dags/__pycache__/
- *.pyc

This keeps the repo clean and portable.


---

