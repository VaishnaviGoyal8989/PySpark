from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FilterMultipleConditions").getOrCreate()
    df = spark.read.csv("data.csv", header=True, inferSchema=True)

    filtered = df.filter((col("department") == "IT") & (col("salary") > 60000) | (col("id") == 3))
    filtered.show()
    spark.stop()
