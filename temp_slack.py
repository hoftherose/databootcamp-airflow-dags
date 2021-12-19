"""Temp slack dag for testing"""
from datetime import datetime

# pylint: disable=no-name-in-module
# pylint: disable=import-error
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

MESSAGE = "Module2: Challenge yourself - Integrate Slack with Airflow (message sent by Hector)"

with DAG(
    "slack_mssg",
    description="Send slack notification",
    schedule_interval="0 12 * * *",
    start_date=datetime(2021, 11, 20),
    catchup=False,
) as dag:
    slack_alert = SlackAPIPostOperator(
        task_id="slack_msg",
        token=BaseHook.get_connection("Slack Connection").password,
        text=MESSAGE,
        channel="#data-engineering-bootcamp",
        dag=dag,
    )
