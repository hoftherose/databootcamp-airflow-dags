"""User purchase dags"""
from datetime import datetime

# pylint: disable=no-name-in-module
# pylint: disable=import-error
from airflow import DAG

from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator,
)
from airflow.contrib.operators.discord_webhook_operator import (
    DiscordWebhookOperator,
)

DAG_NAME = "static_file_gcs"
SRC_BUCKET = "databootcamp-static-files"
DEST_BUCKET = "databootcamp-test1-raw-layer"
OBJECTS = ["data/log_reviews.csv", "data/movie_review.csv"]

with DAG(
    DAG_NAME,
    description="Upload static files to raw layer",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:
    SUCCESS_MESSAGE = f"{DAG_NAME} succeeded at {datetime.now()}"
    FAILURE_MESSAGE = f"{DAG_NAME} failed at {datetime.now()}"

    transfer_logs = GCSToGCSOperator(
        task_id="transfer_log_data",
        source_bucket=SRC_BUCKET,
        source_object=OBJECTS[0],
        destination_bucket=DEST_BUCKET,
        destination_object=OBJECTS[0],
        dag=dag,
    )

    transfer_reviews = GCSToGCSOperator(
        task_id="transfer_review_data",
        source_bucket=SRC_BUCKET,
        source_object=OBJECTS[1],
        destination_bucket=DEST_BUCKET,
        destination_object=OBJECTS[1],
        dag=dag,
    )

    discord_success_alert = DiscordWebhookOperator(
        task_id="discord_msg_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        http_conn_id="discord_default",
        message=SUCCESS_MESSAGE,
        tts=True,
        dag=dag,
    )

    discord_fail_alert = DiscordWebhookOperator(
        task_id="discord_msg_fail",
        trigger_rule=TriggerRule.ONE_FAILED,
        http_conn_id="discord_default",
        message=FAILURE_MESSAGE,
        tts=True,
        dag=dag,
    )

    # pylint: disable=pointless-statement
    (
        transfer_logs
        >> transfer_reviews
        >> (discord_success_alert, discord_fail_alert)
    )
