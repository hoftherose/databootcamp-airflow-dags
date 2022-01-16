from datetime import datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

default_args = {"depends_on_past": False}

# CLUSTER_NAME = "pyspark-cluster"
CLUSTER_NAME = "spark-cluster"
REGION = "us-west1"
PROJECT_ID = "terraformtests-333814"
PYSPARK_LOG_URI = "gs://terraformtests-335517-bucket/spark/logs_etl.py"
PYSPARK_REVIEW_URI = "gs://terraformtests-335517-bucket/spark/review_etl.py"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    num_masters=1,
    num_workers=0,
    master_machine_type="n1-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=30,
    region=REGION,
    gcp_conn_id="GCP Connection",
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
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 3),
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="init_dataproc",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id="GCP Connection",
        cluster_config=CLUSTER_GENERATOR_CONFIG,
    )

    submit_log_job = DataprocSubmitJobOperator(
        task_id="log_load",
        job=PYSPARK_LOG_JOB,
        location=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="GCP Connection",
    )

    submit_review_job = DataprocSubmitJobOperator(
        task_id="review_load",
        job=PYSPARK_REVIEW_JOB,
        location=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="GCP Connection",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id="GCP Connection",
    )

    # pylint: disable=pointless-statement
    create_cluster >> submit_log_job >> submit_review_job >> delete_cluster
