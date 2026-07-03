"""DAG собранный из анти-паттернов для проверки analyze_dag.py."""
from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.settings import Session


def report(**context):
    execution_date = context["execution_date"]
    print(execution_date)


with DAG(
    "antipatterns_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    wait = S3KeySensor(
        task_id="wait_for_file",
        bucket_key="data/{{ ds }}/input.csv",
        poke_interval=300,
        timeout=7200,
    )

    sub = SubDagOperator(task_id="legacy_subdag", subdag=None)
