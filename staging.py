from pyspark.sql.functions import *

GS_PATH = "gs://terraformtests-335517-bucket/"

log_review = spark.read.option("header", "true").csv(GS_PATH + "log_reviews.csv")
movie_review = spark.read.option("header", "true").csv(GS_PATH + "movie_review.csv")

log_review.show(5)
movie_review.show(5)
