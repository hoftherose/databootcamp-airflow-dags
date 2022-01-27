from datetime import datetime

from airflow import DAG

from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)


GCS_PYTHON = "gs://terraformtests-335517-bucket/beam/beam_purchases.py"
DAG_NAME = "database_loader"

with DAG(
    DAG_NAME,
    description="Dag to load log data from raw to staging",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:
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

    # pylint: disable=pointless-statement
    start_python_job
