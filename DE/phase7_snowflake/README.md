# Phase 7 — Snowflake Load: S3 → Snowflake

Phase 7 completes the cloud ELT pipeline by loading the S3-partitioned
CSV files (exported in Phase 6) into Snowflake using an external stage
and `COPY INTO`.

---

## What it does

- Connects to Snowflake using environment-based credentials
- Executes a `COPY INTO` from an S3 external stage (`@WEATHER_S3_STAGE`)
- Uses a pre-configured file format (`RAW_WEATHER_CSV_FORMAT`) to parse CSVs
- Logs the load result row by row
- Verifies the total row count in `raw_weather_hourly` after load
---

## What's in this folder
DE/phase7_snowflake/
scripts/
load_to_snowflake.py
README.md
---

## Snowflake objects required (pre-setup)

Before running this script, the following objects must exist in Snowflake:

**External Stage** pointing to your S3 bucket:
```sql
CREATE OR REPLACE STAGE WEATHER_S3_STAGE
  URL = 's3://datawarehouse-weather-pipeline-kartheek-puttu-2026/'
  CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...')
  FILE_FORMAT = RAW_WEATHER_CSV_FORMAT;
```

**File Format** for CSV parsing:
```sql
CREATE OR REPLACE FILE FORMAT RAW_WEATHER_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');
```

**Target Table:**
```sql
CREATE OR REPLACE TABLE raw_weather_hourly (
  location_name     VARCHAR,
  location_id       INTEGER,
  ts                TIMESTAMP_NTZ,
  temperature_2m    FLOAT,
  precipitation     FLOAT,
  windspeed_10m     FLOAT,
  source            VARCHAR,
  ingested_at       TIMESTAMP_NTZ,
  run_id            INTEGER
);
```
---

## Configuration

All credentials are read from environment variables:

| Variable              | Description                          |
|-----------------------|--------------------------------------|
| `snowflake_user`      | Snowflake username                   |
| `snowflake_password`  | Snowflake password                   |
| `snowflake_account`   | Snowflake account identifier         |
| `snowflake_warehouse` | Virtual warehouse name               |
| `snowflake_database`  | Target database                      |
| `snowflake_schema`    | Target schema                        |

---

## How to run

```bash
python DE/phase7_snowflake/scripts/load_to_snowflake.py
```

Expected output:
Connecting to Snowflake.......
Connected Successfully...
Load result:
('weather_hourly.csv', 'LOADED', 24, 24, 1, 0, None, None, None, None)
...
Total rows in raw_weather_hourly: 72
Snowflake Connection Closed

---

## How the COPY INTO works

```sql
COPY INTO raw_weather_hourly 
FROM @WEATHER_S3_STAGE/raw_weather_hourly/
FILE_FORMAT = RAW_WEATHER_CSV_FORMAT
PATTERN = '.*\.csv';
```

- Scans all `.csv` files recursively under the stage prefix
- Snowflake tracks loaded files internally to avoid reloading the same file
- The `PATTERN` filter ensures only CSV files are picked up

---

## End-to-end flow (all phases)
Open-Meteo API
↓
Phase 3/4 — Incremental load into Postgres (raw_weather_hourly)
↓
Phase 6 — Export Postgres → S3 (partitioned CSV)
↓
Phase 7 — Load S3 → Snowflake (COPY INTO via external stage)

---

## Notes

- Snowflake deduplicates file loads automatically using load history —
  re-running the script on already-loaded files is safe
- Credentials should never be hardcoded; always use environment variables
  or a secrets manager
- This script can be plugged into the Phase 4 Airflow DAG as an
  additional downstream task after the S3 export step
