import os
from pyspark.sql import SparkSession


os.environ["HADOOP_HOME"] = "C:/winutils"
os.environ["PATH"] += os.pathsep + "C:/winutils/bin"


os.environ["spark.hadoop.native.io"] = "false"


os.environ["SPARK_LOCAL_DIRS"] = "C:/tmp"


spark = SparkSession.builder \
    .appName("ReadWriteMultipleFormats") \
    .config("spark.hadoop.tmp.dir", "C:/tmp") \
    .getOrCreate()


df = spark.read.option("header", "true").csv("dataset.csv")


df.write.mode("overwrite").json("C:/spark_output/json")
df.write.mode("overwrite").parquet("C:/spark_output/parquet")
df.write.mode("overwrite").csv("C:/spark_output/csv")

print("Files written successfully in JSON, Parquet, and CSV formats!")

spark.stop()
