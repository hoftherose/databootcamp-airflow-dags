from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

from airflow.contrib.operators.discord_webhook_operator import (
    DiscordWebhookOperator,
)

default_args = {"depends_on_past": False}

# CLUSTER_NAME = "pyspark-cluster"
CLUSTER_NAME = "spark-cluster"
REGION = "us-west1"
PROJECT_ID = "databootcamp-test1"
PYSPARK_LOG_URI = "gs://databootcamp-templates/logs_etl.py"
PYSPARK_REVIEW_URI = "gs://databootcamp-templates/review_etl.py"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    num_masters=1,
    num_workers=0,
    master_machine_type="n1-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=30,
    region=REGION,
).make()


PYSPARK_LOG_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_LOG_URI},
}

PYSPARK_REVIEW_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_REVIEW_URI},
}

DAG_NAME = "log_loader"

with DAG(
    DAG_NAME,
    description="Dag to load log data from raw to staging",
    schedule_interval="0 13 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:
    SUCCESS_MESSAGE = f"{DAG_NAME} succeeded at {datetime.now()}"
    FAILURE_MESSAGE = f"{DAG_NAME} failed at {datetime.now()}"

    create_cluster = DataprocCreateClusterOperator(
        task_id="init_dataproc",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
    )

    submit_log_job = DataprocSubmitJobOperator(
        task_id="log_load",
        job=PYSPARK_LOG_JOB,
        location=REGION,
        project_id=PROJECT_ID,
    )

    submit_review_job = DataprocSubmitJobOperator(
        task_id="review_load",
        job=PYSPARK_REVIEW_JOB,
        location=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    delete_cluster_failed = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_on_fail",
        trigger_rule=TriggerRule.ONE_FAILED,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
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
        create_cluster
        >> submit_log_job
        >> submit_review_job
        >> delete_cluster
        >> (discord_success_alert, discord_fail_alert)
    )

    (
        create_cluster
        >> submit_log_job
        >> submit_review_job
        >> delete_cluster_failed
        >> (discord_success_alert, discord_fail_alert)
    )
