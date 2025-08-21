from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col, max, round

spark = SparkSession.builder.appName("MaxProcessTime").getOrCreate()

data = [("p1", "LoadData", "2024-08-07 08:00:00", "2024-08-07 10:30:00"),
        ("p2", "CleanData", "2024-08-07 11:00:00", "2024-08-07 16:00:00")]
columns = ["process_id", "process_name", "strt_dt_time", "end_dt_time"]
df = spark.createDataFrame(data, columns)

df = df.withColumn("duration_hrs",
    round((unix_timestamp("end_dt_time") - unix_timestamp("strt_dt_time")) / 3600, 2))
df.orderBy(col("duration_hrs").desc()).show(1)

