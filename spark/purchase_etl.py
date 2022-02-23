from pyspark.sql import SparkSession

# the Spark session should be instantiated as follows
spark = (
    SparkSession.builder.appName("SQL basic example")
    .config(
        "spark.jars",
        "gs://databootcamp-templates/jars/jars_postgresql-42.3.1.jar",
    )
    .getOrCreate()
)

project_id = "databootcamp-test1"
saveTo = f"gs://{project_id}-staging-data-layer/purchase_data"

db_ip = "34.83.247.196"
db_name = "purchase"
db_url = f"jdbc:postgresql://{db_ip}:5432/{db_name}"

db_table = "user_purchase"
db_user = "db_user"
db_pass = "dbpassword"

properties = {
    "user": db_user,
    "password": db_pass,
    "driver": "org.postgresql.Driver",
}

rows = spark.read.jdbc(db_url, db_table, properties=properties)

rows.write.option("header", True).csv(saveTo)
