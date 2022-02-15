from typing import NamedTuple
from apache_beam import Pipeline
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.textio import WriteToText
from apache_beam.coders import registry, RowCoder

DB_IP = "34.83.247.196"
DB_USER = "db_user"
DB_PASSWORD = "dbpassword"
GCS_OUTPUT = "gs://staging_data_layer/purchases"

Purchase = NamedTuple("Purchase", [("item_name", float), ("price", float)])
registry.register_coder(Purchase, RowCoder)


with Pipeline() as p:
    result = p | "Read from jdbc" >> ReadFromJdbc(
        table_name="user_purchase",
        driver_class_name="org.postgresql.Driver",
        jdbc_url=f"jdbc:postgresql://{DB_IP}:5432/purchase",
        username=DB_USER,
        password=DB_PASSWORD,
    )

    result | "Write" >> WriteToText(GCS_OUTPUT)
