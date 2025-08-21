from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PivotDataset").getOrCreate()

    sales = spark.read.csv("sales.csv", header=True, inferSchema=True)
    pivoted = sales.groupBy("product").pivot("region").agg(sum("amount"))
    pivoted.show()

    spark.stop()
