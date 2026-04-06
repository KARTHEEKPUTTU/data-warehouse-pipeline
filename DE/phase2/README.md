# Phase 2 – Hourly Weather Ingestion (Open-Meteo API)

> Goal: learn how to pull external data, land it in *raw* tables, and track each run.

---

## What I built

| Layer / object     | Purpose |
|--------------------|---------|
| **dim_location**   | Master list of locations (`location_id`, lat/lon). Upserted so we never duplicate a city. |
| **raw_weather_hourly** | One row per hour per location with temperature, precipitation, windspeed. |
| **ingestion_runs** | Audit log: when the job ran, how many rows, success/failure message. |

---

## How the pipeline works

1. **`load_openmeteo_hourly.py`**  
   * Python script (plain `requests` + `psycopg2` – no frameworks).  
   * Takes `--lat`, `--lon`, and `--name` as CLI args.  
   * Calls the Open-Meteo API, builds a list of rows, bulk-upserts them with `executemany`.

2. **Run tracking**  
   * Inserts a row in `ingestion_runs` *before* calling the API (`status = 'RUNNING'`).  
   * After DB insert, updates the same row with `finished_at`, `row_count`, and `status = 'SUCCESS'`.  
   * Any exception rolls back and leaves the run in `FAILED` state.

3. **Scheduling on Windows**  
   * A thin wrapper `run_openmeteo_chicago.cmd` calls the Python script with Chicago’s coords.  
   * Task Scheduler entry created via  
     ```cmd
     schtasks /Create ^
       /SC DAILY /ST 10:04 ^
       /TN "Phase2_openmeteo_daily" ^
       /TR "\"C:\Kartheek_Space\run_openmeteo_chicago.cmd\"" ^
       /RL HIGHEST /F
     ```
---
## How to test locally

```cmd
REM inside the repo root
python DE\phase2\scripts\load_openmeteo_hourly.py ^
  --lat 41.8781 --lon -87.6298 --name "Chicago, IL"
```

## Expected output
```
location_id = 1 : run_id = 8
SUCCESS: 168 hourly rows upserted (run_id=8)
```
## Then in psql:
```
SELECT COUNT(*) FROM raw_weather_hourly
WHERE run_id = (SELECT MAX(run_id) FROM ingestion_runs);
```
## Next improvements:

Parameterise date range (e.g. back-fill missing days).

Add NOT NULL / range checks on the raw table.

Swap Windows Scheduler for Airflow once we reach Phase 4.
