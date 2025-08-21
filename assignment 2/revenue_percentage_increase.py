from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, round

spark = SparkSession.builder.appName("RevenueIncrease").getOrCreate()

data = [("1", "101", 2023, 50000.0), ("2", "101", 2024, 65000.0), ("3", "102", 2023, 30000.0), ("4", "102", 2024, 39000.0)]
columns = ["id", "org_id", "year", "revenue"]
df = spark.createDataFrame(data, columns)

windowSpec = Window.partitionBy("org_id").orderBy("year")
df = df.withColumn("prev_revenue", lag("revenue").over(windowSpec))
df = df.withColumn("perc_increase", round(((col("revenue") - col("prev_revenue")) / col("prev_revenue")) * 100, 2))
df.show()
