import requests
import psycopg2
import json
import argparse

parser = argparse.ArgumentParser(description = "load hourly weather from open_meteo into postgres")
parser.add_argument("--lat",type = float ,required=True,help = "Latitude, e.g. 41.8781")
parser.add_argument("--lon",type = float ,required=True,help = "Longitude, e.g. -87.6298")
parser.add_argument("--name",type = str ,default=None,help = "Location display name(Optional)")
cli = parser.parse_args()


LAT = cli.lat
LON = cli.lon
LOCATION_NAME = cli.name or f"{LAT:.4f},{LON:.4f}"

def get_conn():
    return psycopg2.connect(
        host = "localhost",
        port = 5432,
        dbname = "de_db",
        user = "de_user",
        password = "postgres",
    )

if __name__ == "__main__":

    params = {
        "latitude": LAT,
        "longitude": LON,
        "note": "hourly weather load -  Phase 2 (no data insert yet)"
    }

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(
        """ INSERT INTO dim_location(location_name,latitude,longitude)
        VALUES (%s,%s,%s)
        ON CONFLICT (latitude,longitude) DO UPDATE
        SET location_name = EXCLUDED.location_name
        RETURNING location_id;
    """,
    (LOCATION_NAME,LAT,LON),
    )
        
    location_id = cur.fetchone()[0]

    cur.execute(
                """
                INSERT INTO ingestion_runs (pipeline_name, source_name, params)
                VALUES (%s, %s, %s)
                RETURNING run_id;
                """,
                ("phase2_openmeteo_hourly", "open-meteo", json.dumps(params)),
            )
    run_id = cur.fetchone()[0]

    conn.commit()

    URL = "https://api.open-meteo.com/v1/forecast"
    HOURLY_VARS = ["temperature_2m", "precipitation", "windspeed_10m"]

    api_params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "America/Chicago",
    }

    resp = requests.get(URL, params=api_params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    times = data["hourly"]["time"]
    temps = data["hourly"].get("temperature_2m", [None] * len(times))
    precs = data["hourly"].get("precipitation",   [None] * len(times))
    winds = data["hourly"].get("windspeed_10m",   [None] * len(times))

    rows = [
        (location_id, t, temps[i], precs[i], winds[i], run_id)
        for i, t in enumerate(times)
    ]

    #upsert weather rows
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO raw_weather_hourly
          (location_id, ts, temperature_2m, precipitation, windspeed_10m, run_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id, ts) DO UPDATE
        SET temperature_2m = EXCLUDED.temperature_2m,
            precipitation  = EXCLUDED.precipitation,
            windspeed_10m  = EXCLUDED.windspeed_10m,
            ingested_at    = NOW(),
            run_id         = EXCLUDED.run_id;
        """,
        rows,
    )
    row_count = len(rows)
    print("location_id = ",location_id, ": run_id = ",run_id)

    #QA guard-rail
    if (row_count != 168) or (t is None for t in temps):
        cur.execute(
            """
                UPDATE ingestion_runs SET finished_at = NOW(),status = 'FAILED',error_message = %s WHERE run_id = %s;
            """,
            (f"Expected 168 rows but got {row_count} rows",run_id)
        )
        conn.rollback()
        raise RuntimeError("Quality check failed - load rolled back")
    
    #mark run SUCCESS
    cur.execute(
        """
        UPDATE ingestion_runs
        SET finished_at = NOW(),
            status      = 'SUCCESS',
            row_count   = %s
        WHERE run_id = %s;
        """,
        (row_count, run_id),
    )

    conn.commit()
    print(f"SUCCESS: {row_count} hourly rows upserted (run_id={run_id})")
    cur.close()
    conn.close()
