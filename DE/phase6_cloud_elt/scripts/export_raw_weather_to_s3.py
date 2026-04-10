import boto3
import os
import csv
import psycopg2
import pendulum
from datetime import timedelta

BUCKET = "datawarehouse-weather-pipeline-kartheek-puttu-2026"
PREFIX = "raw_weather_hourly"

CITIES = [
    ("Chicago, IL","Chicago_IL"),
    ("New York, NY","New_York_NY"),
    ("Los Angeles, CA","Los_Angeles_CA"),
]

# tz=pendulum.timezone("America/Chicago")
# dt=pendulum.now(tz).subtract(days=1).date()
# dt_str=dt.isoformat()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DE_DB_HOST"),
        port=int(os.getenv("DE_DB_PORT","5432")),
        dbname=os.getenv("DE_DB_NAME"),
        user=os.getenv("DE_DB_USER"),
        password=os.getenv("DE_DB_PASSWORD"),
    )

def get_s3_client():
    region=os.getenv("AWS_DEFAULT_REGION")
    return boto3.client("s3",region_name=region)

def export_city_data_to_s3(city_name:str,city_slug:str) -> None:
    s3 = get_s3_client()
    conn=get_conn()
    cur=conn.cursor()
    
    # Asking Postgres what date actually exists for this city
    cur.execute("""
        SELECT MAX(ts::date)
        FROM raw_weather_hourly r
        JOIN dim_location d ON d.location_id = r.location_id
        WHERE d.location_name = %s
    """, (city_name,))

    result = cur.fetchone()[0]
    if not result:
        print(f"NO_DATA: {city_name} (no rows in Postgres)")
        cur.close()
        conn.close()
        return

    dt_str = result.isoformat()

    sql="""
        SELECT d.location_name,
            r.location_id,r.ts,r.temperature_2m,
            r.precipitation,r.windspeed_10m,
            r.source,r.ingested_at,r.run_id
        FROM raw_weather_hourly r 
        JOIN dim_location d 
        ON d.location_id = r.location_id
        WHERE d.location_name=%s
        AND r.ts::date=%s
        ORDER BY r.ts 

        """
    cur.execute(sql,(city_name,dt_str))
    rows = cur.fetchall()

    if not rows:
        print(f"NO_DATA: {city_name} dt={dt_str} (nothing to export)")
        cur.close()
        conn.close()
        return 
    
    local_path = f"/tmp/weather_hourly_{city_slug}_{dt_str}.csv"
    s3_key =  f"{PREFIX}/dt={dt_str}/city={city_slug}/weather_hourly.csv"

    with open(local_path,"w",newline="") as f:
        writer=csv.writer(f)
        writer.writerow([
            "location_name","location_id","ts",
            "temperature_2m","precipitation","windspeed_10m",
            "source","ingested_at","run_id"
        ])
        writer.writerows(rows)


    s3.upload_file(local_path,BUCKET,s3_key)
    os.remove(local_path)

    cur.close()
    conn.close()

    print(f"UPLOADED: s3://{BUCKET}/{s3_key} rows = {len(rows)}")


def main():
    print(f"EXPORT_START bucket={BUCKET} prefix={PREFIX}")
    for city_name,city_slug in CITIES:
        export_city_data_to_s3(city_name,city_slug)
    print("EXPORT_DONE")

if __name__=="__main__":
    main()

