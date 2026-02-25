import requests
import psycopg2
import json
import argparse
from datetime import date,timedelta
import sys

parser = argparse.ArgumentParser(description = "load hourly weather from openmeteo to postgres")
parser.add_argument("--lat",type = float,required = True,help = "Latitude, e.g. 41.8781")
parser.add_argument("--lon",type = float,required = True,help = "Longitude, e.g. -87.6298")
parser.add_argument("--name",type = str,default = None,help = "Location Display Name(Optional)")

cli = parser.parse_args()

LAT = cli.lat
LON = cli.lon
LOCATION_NAME = cli.name or f"{LAT:.4f},{LON:.4f}"

def get_conn():
    return psycopg2.connect(
        host = "host.docker.internal",
        port = 5432,
        dbname = "de_db",
        user = "de_user",
        password = "postgres",
    )

if __name__ == "__main__":
    params = {
        "latitude" : LAT,
        "longitude" : LON,
        "note" : "hourly weather load -  Phase 3 - incremental (no data insert yet)"
    }

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute ("""
                INSERT INTO dim_location(location_name,latitude,longitude) VALUES(%s,%s,%s)
                ON CONFLICT (latitude,longitude) DO UPDATE SET location_name = EXCLUDED.location_name RETURNING location_id 
    """,(LOCATION_NAME,LAT,LON),
    )

    location_id = cur.fetchone()[0]

    cur.execute(
        """
                SELECT MAX(ts) FROM raw_weather_hourly WHERE location_id = %s;
                """,(location_id,),
                )
    last_ts = cur.fetchone()[0]
    if last_ts:
        params["watermark_ts"] = last_ts.isoformat()

    cur.execute("""
                INSERT INTO ingestion_runs(pipeline_name,source_name,params) VALUES(%s,%s,%s) RETURNING run_id
            """,("Phase3_openmeteo_hourly_incremental","open-meteo",json.dumps(params)),
            )
    run_id = cur.fetchone()[0]

    conn.commit()

    URL = "https://api.open-meteo.com/v1/forecast"
    HOURLY_VARS = ["temperature_2m", "precipitation", "windspeed_10m"]

    api_params = {
        "latitude" : LAT,
        "longitude" : LON,
        "hourly" : ",".join(HOURLY_VARS),
        "timezone" : "America/Chicago",
    }
    today = date.today()
    horizon_end = today + timedelta(days=6)

    if last_ts is None:
        # first run → let API default to today … today+6
        pass
    else:
        next_hour  = last_ts + timedelta(hours=1)
        start_date = next_hour.date()

        if start_date > horizon_end:          # ← NEW guard
            cur.execute("""
                UPDATE ingestion_runs
                SET finished_at = NOW(),
                    status      = 'SKIPPED',
                    row_count   = 0,
                    error_message = 'No new hours – watermark beyond forecast window'
                WHERE run_id = %s;
            """, (run_id,))
            conn.commit()
            sys.exit(0)                       # ← graceful exit

        api_params["start_date"] = start_date.isoformat()
        api_params["end_date"]   = horizon_end.isoformat()

    response = requests.get(URL,params = api_params,timeout = 60)
    response.raise_for_status()
    data = response.json()

    times = data["hourly"]["time"]
    temps = data["hourly"].get("temperature_2m",[None] * len(times))
    precs = data["hourly"].get("precipitation", [None] * len(times))
    winds = data["hourly"].get("windspeed_10m", [None] * len(times))

    rows = [
        (location_id,t,temps[i],precs[i],winds[i],run_id)
        for i,t in enumerate(times)
    ]

    #upsert weather rows
    cur = conn.cursor()
    cur.executemany(
        """ INSERT INTO raw_weather_hourly(location_id, ts, temperature_2m, precipitation, windspeed_10m, run_id) 
        VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT (location_id,ts) 
        DO UPDATE SET 
        temperature_2m = EXCLUDED.temperature_2m,
        precipitation = EXCLUDED.precipitation,
        windspeed_10m = EXCLUDED.windspeed_10m,
        ingested_at    = NOW(),
        run_id         = EXCLUDED.run_id;
        """,
        rows,
    )

    # row_count = len(rows)
    
    rows_touched = cur.rowcount
    print("location_id = ",location_id, ": run_id = ",run_id)

    #QA guard - rail
    if (rows_touched == 0) or any(x is None for x in temps):
        conn.rollback()
        cur = conn.cursor()
        cur.execute(
            """ UPDATE ingestion_runs SET finished_at = NOW() ,status = 'FAILED',error_message = %s WHERE run_id = %s;
            """,(f"QC failed:rows_touched ={rows_touched} ",run_id),
        ) 
        conn.commit()
        raise RuntimeError("Quality Check failed - load rolled back")
    
    cur.execute(
        """UPDATE ingestion_runs SET finished_at = NOW(), status = 'SUCCESS', row_count = %s,error_message = NULL WHERE run_id = %s;
        """,(rows_touched,run_id),

    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"SUCCESS: {rows_touched} rows upserted (run_id = {run_id})")


    