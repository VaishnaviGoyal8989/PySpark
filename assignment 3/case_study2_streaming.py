from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("StreamingLogAnalytics") \
    .getOrCreate()

# Schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("watch_time", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read streaming CSV
logs_df = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv("log_stream_data/")

# Filter watch_time >= 2
filtered_df = logs_df.filter(col("watch_time") >= 2)

# Total watch time per user (5-min window) 
user_watch_df = filtered_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes"), col("user_id")) \
    .agg(_sum("watch_time").alias("total_watch_time"))

user_query = user_watch_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Top 5 most-watched videos (10-min window) 
video_watch_df = filtered_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "10 minutes"), col("video_id")) \
    .agg(_sum("watch_time").alias("total_watch_time"))

# Console output
video_query_console = video_watch_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Parquet output 
video_query_parquet = video_watch_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "parquet_video_watch") \
    .option("checkpointLocation", "checkpoint_video_watch") \
    .start()

spark.streams.awaitAnyTermination()


