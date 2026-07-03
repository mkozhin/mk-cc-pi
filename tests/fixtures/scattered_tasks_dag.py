"""DAG с тасками, разбросанными по телу DAG-блока, для проверки analyze_dag.py."""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    return {"data": [1, 2, 3]}


def validate(**context):
    return True


def load(**context):
    return None


with DAG(
    "scattered_tasks_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    THRESHOLD = 100

    def _log_threshold():
        print(THRESHOLD)

    validate_task = PythonOperator(task_id="validate", python_callable=validate)
    extract_task >> validate_task

    load_task = PythonOperator(task_id="load", python_callable=load)
    validate_task >> load_task
