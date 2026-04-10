FROM apache/airflow:2.9.0-python3.12
RUN pip install --no-cache-dir "apache-airflow[celery]==2.9.0" redis snowflake-connector-python
USER airflow