"""DAG с корректным deferrable-сенсором для проверки analyze_dag.py."""
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

with DAG(
    "sensor_deferrable_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    wait = S3KeySensor(
        task_id="wait_for_file",
        bucket_key="data/{{ ds }}/input.csv",
        deferrable=True,
    )
