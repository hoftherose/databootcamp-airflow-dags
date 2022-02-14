import os
import sys

# sys.path.append("/opt/airflow/dags/repo/dags")

try:
    from af_dags.db_upload.hooks import GCSToPostgresTransfer
except ImportError as Err:
    raise Exception(sys.path)

from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.contrib.operators.discord_webhook_operator import (
    DiscordWebhookOperator,
)

DAG_NAME = "debugger"

with DAG(
    DAG_NAME,
    description="Dag to load log data from raw to staging",
    schedule_interval="0 13 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:
    SUCCESS_MESSAGE = f"{DAG_NAME} succeeded at {datetime.now()}"
    FAILURE_MESSAGE = f"{DAG_NAME} failed at {datetime.now()}"

    discord_success_alert = DiscordWebhookOperator(
        task_id="discord_msg_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        http_conn_id="Discord Connection",
        message=SUCCESS_MESSAGE,
        tts=True,
        dag=dag,
    )

    discord_fail_alert = DiscordWebhookOperator(
        task_id="discord_msg_fail",
        trigger_rule=TriggerRule.ONE_FAILED,
        http_conn_id="Discord Connection",
        message=FAILURE_MESSAGE,
        tts=True,
        dag=dag,
    )

    # pylint: disable=pointless-statement
    discord_success_alert
