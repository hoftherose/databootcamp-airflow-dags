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
    schedule_interval=None,
    start_date=datetime(2021, 12, 20),
    catchup=False,
) as dag:
    create_user_table = PostgresOperator(
        task_id="create_user_purchase_table",
        sql=CREATE_USER_PURCHASE_TABLE,
    )

    gcs2postgres = GCSToPostgresTransfer(
        task_id="gcs_to_postgres",
        database="purchase",
        table="user_purchase",
        bucket="terraformtests-333814-bucket",
        object_name="user_purchase.csv",
        google_conn_id="google_cloud_default",
        postgres_conn_id="postgres_default",
        dag=dag,
    )

    create_user_table >> gcs2postgres
