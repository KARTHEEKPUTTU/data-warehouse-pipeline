import os
import snowflake.connector

SNOWFLAKE_USER = os.getenv("snowflake_user")
SNOWFLAKE_PASSWORD = os.getenv("snowflake_password")
SNOWFLAKE_ACCOUNT = os.getenv("snowflake_account")
SNOWFLAKE_WAREHOUSE = os.getenv("snowflake_warehouse")
SNOWFLAKE_DATABASE = os.getenv("snowflake_database")
SNOWFLAKE_SCHEMA = os.getenv("snowflake_schema")

def load_data_to_sbowflake():
    conn = None
    cur = None

    try:
        print("Connecting to Snowflake.......")
        
        conn = snowflake.connector.connect(
            user = SNOWFLAKE_USER,
            password = SNOWFLAKE_PASSWORD,
            account = SNOWFLAKE_ACCOUNT,
            warehouse = SNOWFLAKE_WAREHOUSE,
            database = SNOWFLAKE_DATABASE,
            schema = SNOWFLAKE_SCHEMA
        )

        cur = conn.cursor()
        print("Connected Successfully...")

        cur.execute(
            """
            COPY INTO raw_weather_hourly FROM @WEATHER_S3_STAGE/raw_weather_hourly/ 
            FILE_FORMAT = RAW_WEATHER_CSV_FORMAT PATTERN = '.*\\.csv';   
            """
        )

        results = cur.fetchall()
        print("Load result:")
        for row in results:
            print(row)
        
        count_query = """ SELECT COUNT(*) FROM raw_weather_hourly; """
        cur.execute(count_query)
        total_rows = cur.fetchone()[0]

        print("Total rows in raw_weather_hourly:",total_rows)

    except Exception as e:
        print("Error while loading into snowflake",e)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Snowflake Connection Closed")
    
if __name__ == "__main__":
    load_data_to_sbowflake()
