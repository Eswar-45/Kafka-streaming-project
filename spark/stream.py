from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("KafkaStreamingDemo")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("city", StringType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("event_time", StringType())
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "order-events")
    .option("startingOffsets", "latest")
    .load()
)

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

parsed_df = (
    json_df
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

query = (
    parsed_df.writeStream
    .format("delta")
    .option("path", "/opt/data/orders")
    .option("checkpointLocation", "/opt/data/checkpoints/orders")
    .outputMode("append")
    .start()
)

query.awaitTermination()

