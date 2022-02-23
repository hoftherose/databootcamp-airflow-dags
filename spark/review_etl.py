from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import when

sc = SparkContext("local")
spark = SparkSession(sc)

project_id = "databootcamp-test1"
file = f"gs://{project_id}-raw-layer/data/movie_review.csv"
saveTo = f"gs://{project_id}-staging-data-layer/movie_review"
lines = spark.read.option("header", True).csv(file)


reviews = lines.withColumn(
    "positive_review", when(lines.review_str.contains("good"), 1).otherwise(0)
)
reviews = reviews.withColumnRenamed("cid", "user_id").withColumnRenamed(
    "id_review", "review_id"
)


reviews[
    [
        "user_id",
        "review_id",
        "positive_review",
    ]
].write.option("header", True).csv(saveTo)
