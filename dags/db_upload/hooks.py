"""Custom dag hooks"""
from typing import Optional, Union, Sequence, Any

# pylint: disable=no-name-in-module
# pylint: disable=import-error
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd


class GCSToPostgresTransfer(BaseOperator):
    """Object to upload from GCS to Postgres database"""

    COLUMN_MAPPER = {
        "InvoiceNo": "invoice_number",
        "StockCode": "stock_code",
        "Description": "detail",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
    }

    DATA_SCHEME = {
        "invoice_number": "string",
        "stock_code": "string",
        "detail": "string",
        "quantity": "Int64",
        "invoice_date": "string",
        "unit_price": "float",
        "customer_id": "Int64",
        "country": "string",
    }
    # pylint: disable=keyword-arg-before-vararg
    def __init__(
        self,
        schema: str,
        table: str,
        bucket: str,
        object_name: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        google_cloud_storage_conn_id: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        postgres_conn_id: str = None,
        *args,
        **kwargs,
    ):
        super(GCSToPostgresTransfer, self).__init__(*args, **kwargs)
        self.table = table
        self.schema = schema
        self.bucket = bucket
        self.object = object_name

        self.gcs_hook = GCSHook(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            google_cloud_storage_conn_id=google_cloud_storage_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.pg_hook = PostgresHook(postgres_conn_id, **kwargs)

    def execute(self, context: Any):
        self.log.info("Retrieving %s from %s bucket", self.object, self.bucket)
        with self.get_gcs_file() as file:
            df_products = pd.read_csv(
                file,
                sep=",",
                low_memory=False,
                dtype=self.DATA_SCHEME,
            )
        self.log.info(df_products)
        self.log.info(df_products.info())
        self.upload_df_to_pg(df_products)

    def get_gcs_file(self):
        """Get the gcs file object"""
        if not self.gcs_hook.exists(self.bucket, self.object):
            raise AirflowException(
                f"Could not find {self.object} from bucket {self.bucket}"
            )
        return self.gcs_hook.provide_file(
            bucket_name=self.bucket,
            object_name=self.object,
        )

    def upload_df_to_pg(self, data: pd.DataFrame):
        """Upload dataframe to pg database"""
        self.log.info("Preping dataframe for upload")
        data.rename(
            mapper=self.COLUMN_MAPPER,
            axis=1,
            inplace=True,
        )
        data.detail.fillna("", inplace=True)
        self.log.info("Inserting rows into database")

        insert_data = list(map(self._format_to_sql, data.values.tolist()))
        self.pg_hook.insert_rows(
            table=f"{self.schema}.{self.table}",
            rows=insert_data,
            target_fields=self.COLUMN_MAPPER.values(),
            commit_every=10000,
            replace=False,
        )
        self.log.info("uploaded dataframe to database")

    def _format_to_sql(self, row: list):
        if pd.isna(row[6]):
            row[6] = None
        return tuple(row)
