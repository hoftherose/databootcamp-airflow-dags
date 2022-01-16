from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
)

default_args = {"depends_on_past": False}

CLUSTER_NAME = "pyspark_cluster"
REGION = "us-west1"
PROJECT_ID = "terraformtests-335517"
PYSPARK_URI = f"gs://{PROJECT_ID}-bucket/spark/logs_etl.py"


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

DAG_NAME = "log_loader"

with DAG(
    DAG_NAME,
    description="Dag to load log data from raw to staging",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="init_dataproc",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id="temp",
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="log_load",
        job=PYSPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="temp",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id="temp",
    )

    create_cluster >> submit_job >> delete_cluster
