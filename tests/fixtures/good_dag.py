"""Пример корректного DAG для smoke-теста analyze_dag.py."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="good_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
    doc_md=__doc__,
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
        "email_on_failure": True,
        "email": ["alerts@example.com"],
    },
) as dag:

    @task
    def extract():
        return {"ds": "{{ ds }}"}

    extract()
