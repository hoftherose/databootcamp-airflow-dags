from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import array_contains, when
from pyspark.ml.feature import Tokenizer, StopWordsRemover

sc = SparkContext("local")
spark = SparkSession(sc)

file = "gs://raw_layer/movie_review.csv"
saveTo = "gs://staging_data_layer/movie_review.csv"
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
].write.csv(saveTo)
