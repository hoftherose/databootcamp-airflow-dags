"""User purchase dags"""
from datetime import datetime

# pylint: disable=no-name-in-module
# pylint: disable=import-error
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from custom.hooks import GCSToPostgresTransfer
from sql.create_table import CREATE_USER_PURCHASE_TABLE

with DAG(
    "user_purchase_gcs_to_postgres",
    description="Upload purchase data from GCS to postgres",
    schedule_interval="0 12 * * *",
    start_date=datetime(2021, 11, 20),
    catchup=False,
) as dag:
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
        object_name="user-purchase/user_purchase.csv",
        gcp_conn_id="GCP Connection",
        postgres_conn_id="Database connection",
        dag=dag,
    )

    create_user_table >> gcs2postgres
