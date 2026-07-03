"""Пример DAG с синтаксической ошибкой для smoke-теста analyze_dag.py."""
from airflow import DAG

with DAG("bad_dag" as dag:
    pass
