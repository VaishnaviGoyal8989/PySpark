from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName("ModifyAndWriteToHDFS").getOrCreate()

data = [("1", "John", 50000, "HR"),
        ("2", "Alice", 60000, "IT"),
        ("3", "Bob", 55000, "HR")]
columns = ["empid", "empname", "salary", "department"]
df = spark.createDataFrame(data, columns)

df_updated = df.withColumn("salary",
    when(col("department") == "HR", col("salary") * 0.9).otherwise(col("salary")))

df_updated.write.mode("overwrite").csv("hdfs://localhost:9000/output/employees")
