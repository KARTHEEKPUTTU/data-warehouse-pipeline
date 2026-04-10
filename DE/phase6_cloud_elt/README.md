# Phase 6 — Cloud ELT: Postgres → AWS S3 Export

Phase 6 adds a cloud export layer to the pipeline. It reads the latest
hourly weather data from Postgres and uploads it to AWS S3 as
partitioned CSV files — making the data available for downstream
cloud-based tools like Snowflake (Phase 7).

---

## What it does

- Connects to Postgres and finds the latest available date per city
- Exports that day's hourly rows as a CSV file
- Uploads to S3 using a partitioned key structure:
  `raw_weather_hourly/dt=YYYY-MM-DD/city=City_Slug/weather_hourly.csv`
- Cleans up the local temp file after upload
- Handles missing data gracefully (NO_DATA log, clean exit)

---

## What's in this folder
DE/phase6_cloud_elt/
scripts/
export_to_s3.py
README.md

---

## S3 Structure
s3://datawarehouse-weather-pipeline-kartheek-puttu-2026/
raw_weather_hourly/
dt=2026-04-09/
city=Chicago_IL/
weather_hourly.csv
city=New_York_NY/
weather_hourly.csv
city=Los_Angeles_CA/
weather_hourly.csv

The partition layout (`dt=` / `city=`) follows Hive-style conventions,
making it compatible with Snowflake external stages, AWS Glue, and Athena.

---

## Cities exported

| City             | Slug             |
|------------------|------------------|
| Chicago, IL      | Chicago_IL       |
| New York, NY     | New_York_NY      |
| Los Angeles, CA  | Los_Angeles_CA   |

---

## Configuration

All credentials are read from environment variables. Do not hardcode these.

| Variable           | Description                        |
|--------------------|------------------------------------|
| `DE_DB_HOST`       | Postgres host                      |
| `DE_DB_PORT`       | Postgres port (default: 5432)      |
| `DE_DB_NAME`       | Database name                      |
| `DE_DB_USER`       | Database user                      |
| `DE_DB_PASSWORD`   | Database password                  |
| `AWS_DEFAULT_REGION` | AWS region (e.g. `us-east-1`)   |

AWS credentials are picked up automatically via:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- Or an AWS credentials file / IAM role

---

## How to run

```bash
python DE/phase6_cloud_elt/scripts/export_to_s3.py
```

Expected output:
EXPORT_START bucket=datawarehouse-weather-pipeline-kartheek-puttu-2026 prefix=raw_weather_hourly
UPLOADED: s3://datawarehouse-weather-pipeline-kartheek-puttu-2026/raw_weather_hourly/dt=2026-04-09/city=Chicago_IL/weather_hourly.csv rows=24
UPLOADED: s3://...New_York_NY/weather_hourly.csv rows=24
UPLOADED: s3://...Los_Angeles_CA/weather_hourly.csv rows=24
EXPORT_DONE

---

## Design notes

- The script queries Postgres for `MAX(ts::date)` per city rather than
  using a hardcoded date — so it always exports what actually exists
- Rows are written to `/tmp/` locally, uploaded to S3, then deleted
- If a city has no data, the script logs `NO_DATA` and moves on without failing
- This script is designed to run after the Phase 4 Airflow ingestion completes

---

## What Phase 7 does with this

Phase 7 reads these S3 CSV files into Snowflake using an external stage
and `COPY INTO`, completing the Postgres → S3 → Snowflake ELT flow.
