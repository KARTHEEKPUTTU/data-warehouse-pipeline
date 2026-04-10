from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
import pendulum 

tz = pendulum.timezone("America/Chicago")
with DAG(
    dag_id="openmeteo_incremental",
    start_date=pendulum.datetime(2026,2,23,tz=tz),
    schedule="00 4 * * *",
    catchup=False,
    tags=["weather","phase4"],

) as dag:
    run_chicago = BashOperator(
        task_id="run_openmeteo_loader_chicago",
        bash_command=(
            'python /opt/airflow/phase3/scripts/load_openmeteo_hourly_incremental.py '
            '--lat 41.8781 --lon -87.6298 --name "Chicago, IL"'
        ),
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )
    run_nyc=BashOperator(
        task_id="run_openmeteo_nyc",
        bash_command=(
            'python /opt/airflow/phase3/scripts/load_openmeteo_hourly_incremental.py '
            '--lat 40.7128 --lon -74.0060 --name "New York, NY"'
        ),
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )
    run_la=BashOperator(
        task_id="run_openmeteo_la",
        bash_command=(
            'python /opt/airflow/phase3/scripts/load_openmeteo_hourly_incremental.py '
            '--lat 34.0522 --lon -118.2437 --name "Los Angeles, CA" ' 
        ),
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )
    qc_check=BashOperator(
        task_id="qc_check_latest_run",
        bash_command="python /opt/airflow/dags/qc_openmeteo.py",
        env={
            "QC_RUN_TS": "{{ dag_run.start_date }}",
            "DE_DB_HOST": "host.docker.internal",
            "DE_DB_PORT": "5432",
            "DE_DB_NAME": "de_db",
            "DE_DB_USER": "de_user",
            "DE_DB_PASSWORD": "postgres",
        },
    )
    export_to_s3=BashOperator(
        task_id="export_raw_weather_to_s3",
        bash_command="python /opt/airflow/phase6_cloud_elt/scripts/export_raw_weather_to_s3.py",
        env={
        "EXPORT_DATE": "{{ ds }}",
        },
        append_env=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )
    load_raw_weather_to_snowflake=BashOperator(
        task_id="load_raw_weather_to_snowflake",
        bash_command="python /opt/airflow/phase7_snowflake/scripts/load_raw_weather_to_snowflake.py",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10)
    )
[run_chicago,run_nyc,run_la] >> qc_check >> export_to_s3 >> load_raw_weather_to_snowflake
