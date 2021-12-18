"""Airflow definitions"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)


def print_hello():
    """Print hello and leave :V"""
    return "Hello world from first Airflow DAG from hoftherose!"


def print_bye():
    """Print hello and leave :V"""
    return "Good bye world from first Airflow DAG from hoftherose!"


with DAG(
    "hello_world",
    description="Hello World DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
) as dag:

    hello_operator = PythonOperator(
        task_id="hello_task", python_callable=print_hello, dag=dag
    )

    bye_operator = PythonOperator(
        task_id="bye_task", python_callable=print_bye, dag=dag
    )

    hello_operator >> bye_operator
