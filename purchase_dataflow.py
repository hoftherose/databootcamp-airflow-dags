from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)
from airflow.contrib.operators.discord_webhook_operator import (
    DiscordWebhookOperator,
)


GCS_PYTHON = "gs://terraformtests-335517-bucket/beam/beam_purchases.py"
DAG_NAME = "database_loader"

with DAG(
    DAG_NAME,
    description="Dag to load log data from raw to staging",
    schedule_interval="0 14 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:
    SUCCESS_MESSAGE = f"{DAG_NAME} succeeded at {datetime.now()}"
    FAILURE_MESSAGE = f"{DAG_NAME} failed at {datetime.now()}"

    start_python_job = BeamRunPythonPipelineOperator(
        task_id="start_dataflow",
        py_file=GCS_PYTHON,
        runner="DataflowRunner",
        py_options=[],
        py_requirements=["apache-beam[gcp]==2.21.0"],
        py_interpreter="python3",
        dataflow_config={"location": "us-west1"},
        gcp_conn_id="GCP Connection",
    )

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
    start_python_job >> (discord_success_alert, discord_fail_alert)
