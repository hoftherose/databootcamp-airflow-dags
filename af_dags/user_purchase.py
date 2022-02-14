"""User purchase dags"""
from datetime import datetime

# pylint: disable=no-name-in-module
# pylint: disable=import-error
from airflow import DAG

from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.discord_webhook_operator import (
    DiscordWebhookOperator,
)

from db_upload.hooks import GCSToPostgresTransfer
from sql.create_table import CREATE_USER_PURCHASE_TABLE

DAG_NAME = "user_purchase_gcs_to_postgres"

with DAG(
    DAG_NAME,
    description="Upload purchase data from GCS to postgres",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:
    SUCCESS_MESSAGE = f"{DAG_NAME} succeeded at {datetime.now()}"
    FAILURE_MESSAGE = f"{DAG_NAME} failed at {datetime.now()}"

    create_user_table = PostgresOperator(
        task_id="create_user_purchase_table",
        sql=CREATE_USER_PURCHASE_TABLE,
        dag=dag,
    )

    gcs2postgres = GCSToPostgresTransfer(
        task_id="gcs_to_postgres",
        schema="public",
        table="user_purchase",
        bucket="databootcamp-static-files",
        object_name="data/user_purchase.csv",
        dag=dag,
    )

    discord_success_alert = DiscordWebhookOperator(
        task_id="discord_msg_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        http_conn_id="discord_connection",
        message=SUCCESS_MESSAGE,
        tts=True,
        dag=dag,
    )

    discord_fail_alert = DiscordWebhookOperator(
        task_id="discord_msg_fail",
        trigger_rule=TriggerRule.ONE_FAILED,
        http_conn_id="discord_connection",
        message=FAILURE_MESSAGE,
        tts=True,
        dag=dag,
    )

    # pylint: disable=pointless-statement
    (
        create_user_table
        >> gcs2postgres
        >> (discord_success_alert, discord_fail_alert)
    )
