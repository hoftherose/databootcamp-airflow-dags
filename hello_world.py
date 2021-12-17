"""Airflow definitions"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    """Print hello and leave :V"""
    return 'Hello world from first Airflow DAG from hoftherose version 2!'


dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(
    task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
