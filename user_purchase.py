"""User purchase dags"""
from datetime import datetime

# pylint: disable=no-name-in-module
# pylint: disable=import-error
from airflow import DAG

from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from custom.hooks import GCSToPostgresTransfer
from sql.create_table import CREATE_USER_PURCHASE_TABLE

DAG_NAME = "user_purchase_gcs_to_postgres"

with DAG(
    DAG_NAME,
    description="Upload purchase data from GCS to postgres",
    schedule_interval="0 12 * * *",
    start_date=datetime(2021, 11, 20),
    catchup=False,
) as dag:
    SUCCESS_MESSAGE = f"{DAG_NAME} succeeded at {datetime.now()}"
    FAILURE_MESSAGE = f"{DAG_NAME} failed at {datetime.now()}"

    create_user_table = PostgresOperator(
        task_id="create_user_purchase_table",
        sql=CREATE_USER_PURCHASE_TABLE,
        postgres_conn_id="Database connection",
        dag=dag,
    )

    gcs2postgres = GCSToPostgresTransfer(
        task_id="gcs_to_postgres",
        schema="public",
        table="user_purchase",
        bucket="terraformtests-335517-bucket",
        object_name="user-purchase/user_purchase_sample.csv",
        gcp_conn_id="GCP Connection",
        postgres_conn_id="Database connection",
        dag=dag,
    )

    slack_success_alert = SlackWebhookOperator(
        task_id="slack_msg_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        http_conn_id="Slack Connection",
        webhook_token=BaseHook.get_connection("Slack Connection").password,
        message=SUCCESS_MESSAGE,
        dag=dag,
    )

    slack_fail_alert = SlackWebhookOperator(
        task_id="slack_msg_fail",
        trigger_rule=TriggerRule.ONE_FAILED,
        http_conn_id="Slack Connection",
        webhook_token=BaseHook.get_connection("Slack Connection").password,
        message=FAILURE_MESSAGE,
        dag=dag,
    )

    create_user_table >> gcs2postgres >> (slack_success_alert, slack_fail_alert)
