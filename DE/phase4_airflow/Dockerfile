FROM apache/airflow:2.9.0-python3.12
# USER root
RUN pip install --no-cache-dir "apache-airflow[celery]==2.9.0" redis
USER airflow