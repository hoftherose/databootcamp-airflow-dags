from pyspark.context import SparkContext
from pyspark.sql.functions import monotonically_increasing_id, sum, count
from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType

sc = SparkContext("local")
spark = SparkSession(sc)

project_id = "databootcamp-test1"

silver_path = f"gs://{project_id}-staging-data-layer"
gold_path = f"gs://{project_id}-feature-layer"

logs = spark.read.option("header", True).csv(silver_path + "/log_reviews")
reviews = spark.read.option("header", True).csv(silver_path + "/movie_review")
purchases = spark.read.option("header", True).csv(
    silver_path + "/purchase_data"
)

# dim_devices
dim_device = logs[["device"]].distinct()
dim_device = dim_device.withColumn("device_id", monotonically_increasing_id())
dim_device = dim_device.withColumnRenamed("device", "name")
dim_device[["device_id", "name"]].write.option("header", True).csv(
    gold_path + "/devices"
)
# dim_device[["device_id", "name"]].show(5)

# dim_os
dim_os = logs[["os"]].distinct()
dim_os = dim_os.withColumn("os_id", monotonically_increasing_id())
dim_os = dim_os.withColumnRenamed("os", "name")
dim_os[["os_id", "name"]].write.option("header", True).csv(gold_path + "/os")
# dim_os[["os_id", "name"]].show(5)

# dim_date
dim_date = logs[["log_date"]].distinct()
dim_date = dim_date.withColumn("date_id", monotonically_increasing_id())
dim_date = dim_date.withColumnRenamed("log_date", "name")
dim_date[["date_id", "name"]].write.option("header", True).csv(
    gold_path + "/date"
)
# dim_date[["date_id", "name"]].show(5)

# dim_location
dim_location = logs[["location"]].distinct()
dim_location = dim_location.withColumn(
    "location_id", monotonically_increasing_id()
)
dim_location = dim_location.withColumnRenamed("location", "name")
dim_location[["location_id", "name"]].write.option("header", True).csv(
    gold_path + "/location"
)
# dim_location[["location_id", "name"]].show(5)

# dim_browser
dim_browser = logs[["browser"]].distinct()
dim_browser = dim_browser.withColumn(
    "browser_id", monotonically_increasing_id()
)
dim_browser = dim_browser.withColumnRenamed("browser", "name")
dim_browser[["browser_id", "name"]].write.option("header", True).csv(
    gold_path + "/browser"
)
# dim_browser[["browser_id", "name"]].show(5)

review = reviews.join(logs, reviews.review_id == logs.log_id)
review = review.join(dim_location, review.location == dim_location.name)
review = review.join(dim_browser, review.browser == dim_browser.name)
review = review.join(dim_device, review.device == dim_device.name)
review = review.join(dim_date, review.log_date == dim_date.name)
review = review.join(dim_os, review.os == dim_os.name)

review = review[
    [
        "review_id",
        "user_id",
        "positive_review",
        "location_id",
        "device_id",
        "date_id",
        "os_id",
    ]
]
review = review.withColumn(
    "positive_review", review["positive_review"].cast(IntegerType())
)
review[
    [
        "review_id",
        "user_id",
        "positive_review",
        "location_id",
        "browser_id",
        "device_id",
        "date_id",
        "os_id",
    ]
].write.option("header", True).csv(gold_path + "/review")

purchases = purchases.withColumn(
    "unit_price", purchases["unit_price"].cast(IntegerType())
)
purchases = purchases.withColumn(
    "quantity", purchases["quantity"].cast(IntegerType())
)
purchases = purchases.withColumn(
    "money_spent", purchases.quantity * purchases.unit_price
)
customer = purchases.groupBy("customer_id").sum("money_spent")

temp_table = review.groupBy("user_id").agg(
    sum("positive_review"), count("positive_review")
)
customer = customer.join(
    temp_table, customer.customer_id == temp_table.user_id
)
customer = customer.withColumnRenamed("sum(money_spent)", "amount_spent")
customer = customer.withColumnRenamed(
    "sum(positive_review)", "num_pos_reviews"
)
customer = customer.withColumnRenamed("count(positive_review)", "num_reviews")
customer = customer[
    ["customer_id", "amount_spent", "num_reviews", "num_pos_reviews"]
]

customer[
    ["customer_id", "amount_spent", "num_pos_reviews", "num_reviews"]
].write.option("header", True).csv(gold_path + "/customer")
