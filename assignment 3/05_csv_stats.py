from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSVStats").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df.select(avg("salary").alias("avg_salary"),
              min("salary").alias("min_salary"),
              max("salary").alias("max_salary")).show()

    spark.stop()
