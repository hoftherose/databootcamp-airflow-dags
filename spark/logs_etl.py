from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import xml.etree.ElementTree as xml

sc = SparkContext("local")
spark = SparkSession(sc)

project_id = "databootcamp-test1"
file = f"gs://{project_id}-raw-layer/data/log_reviews.csv"
saveTo = f"gs://{project_id}-staging-data-layer/log_reviews"
lines = spark.read.option("header", True).csv(file)


def xml_to_data(row):
    log_tree = xml.fromstring(row.log)
    dic = {}
    for child in log_tree[0]:
        dic[child.tag] = child.text
    return Row(
        log_id=row.id_review,
        log_date=dic["logDate"],
        device=dic["device"],
        os=dic["os"],
        location=dic["location"],
        ip=dic["ipAddress"],
        phone_number=dic["phoneNumber"],
    )


processed = lines.select("*").rdd.map(xml_to_data).toDF()
processed[
    [
        "log_id",
        "log_date",
        "device",
        "os",
        "location",
        "ip",
        "phone_number",
    ]
].write.csv(saveTo)
