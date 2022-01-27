from typing import NamedTuple
from apache_beam import Pipeline
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.textio import WriteToText
from apache_beam.coders import registry, RowCoder

GCS_OUTPUT = "gs://staging_data_layer/purchases"


Purchase = NamedTuple("Purchase", [("item_name", float), ("price", float)])
registry.register_coder(Purchase, RowCoder)


with Pipeline() as p:
    result = p | "Read from jdbc" >> ReadFromJdbc(
        table_name="user_purchase",
        driver_class_name="org.postgresql.Driver",
        jdbc_url="jdbc:postgresql://34.127.48.25:5432/purchase",
        username="db_user",
        password="dbpassword",
    )

    result | "Write" >> WriteToText(GCS_OUTPUT)
