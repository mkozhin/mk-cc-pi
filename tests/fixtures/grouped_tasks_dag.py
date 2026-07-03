"""DAG с тасками, сгруппированными в одном блоке внизу, для проверки analyze_dag.py."""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

THRESHOLD = 100


def extract():
    return {"data": [1, 2, 3]}


def validate(**context):
    return True


def load(**context):
    return None


with DAG(
    "grouped_tasks_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    validate_task = PythonOperator(task_id="validate", python_callable=validate)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> validate_task >> load_task
