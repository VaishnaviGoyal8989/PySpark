from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FilterNulls").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df_clean = df.filter((col("name").isNotNull()) & (col("name") != ""))
    df_clean.show()

    spark.stop()
