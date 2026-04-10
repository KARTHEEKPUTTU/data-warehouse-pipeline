import os
import sys
import psycopg2
from datetime import datetime,timedelta,timezone

PIPELINE_NAME="Phase3_openmeteo_hourly_incremental"

def main():
    conn=psycopg2.connect(
        host=os.environ["DE_DB_HOST"],
        port=int(os.environ.get("DE_DB_PORT","5432")),
        dbname=os.environ["DE_DB_NAME"],
        user=os.environ["DE_DB_USER"],
        password=os.environ["DE_DB_PASSWORD"],
    )
    cur=conn.cursor()

    run_ts=os.environ.get("QC_RUN_TS")
    if not run_ts:
        print("QC FAILED: QC_RUN_TS not provided")
        sys.exit(1)
    
    run_dt=datetime.fromisoformat(run_ts)

    window_start = run_dt 
    window_end = run_dt + timedelta(hours=2)

    cur.execute(
        """
            SELECT run_id,status,row_count,finished_at 
            FROM ingestion_runs WHERE pipeline_name=%s
            AND started_at >= %s
            AND started_at <= %s
            ORDER BY run_id DESC
            LIMIT 1
        """,(PIPELINE_NAME,window_start,window_end)
    )

    row = cur.fetchone()
    if not row:
        print(f"QC FAILED: No ingestion_runs found in window {window_start} to {window_end} for {PIPELINE_NAME}")
        sys.exit(1)
    
    run_id,status,row_count,finished_at = row
    print(f"QC: latest run_id={run_id}, status={status}, row_count={row_count}, finished_at={finished_at}")
    
    
    if status not in ("SUCCESS","SKIPPED"):
        print(f"QC FAILED:status is {status},expected SUCCESS or SKIPPED")
        sys.exit(1)

    if status=="SUCCESS":
        if finished_at is None:
            print("QC FAILED: SUCCESS run has finished_at = NULL")
            sys.exit(1)
        if row_count is None or row_count<=0:
            print(f"QC Failed: SUCCESS run has row_count={row_count},expecte > 0")
            sys.exit(1)

        cur.execute("""
            SELECT COUNT(*) FROM raw_weather_hourly WHERE run_id=%s 
            AND (ts IS NULL OR temperature_2m IS NULL OR precipitation IS NULL OR windspeed_10m IS NULL)                  
        """,(run_id,)
        ) 

        null_count=cur.fetchone()[0]
        if null_count>0:
            print(f"QC FAILED: FOUND {null_count} rows with NULLS for run_id={run_id}")
            sys.exit(1)

        cur.execute(
            """
            SELECT COUNT(*) - COUNT(DISTINCT(location_id,ts)) FROM raw_weather_hourly WHERE run_id = %s
        """,(run_id,)
        )
        
        duplicate_count = cur.fetchone()[0]
        if duplicate_count>0:
           print(f"QC FAILED: FOUND {duplicate_count} duplicate pairs for run_id={run_id}")
           sys.exit(1)

    cur.close()
    conn.close()
    print("QC PASSED")

if __name__ == "__main__":
    main()