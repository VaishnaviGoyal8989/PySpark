from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TotalSales").getOrCreate()

    sales = spark.read.csv("sales.csv", header=True, inferSchema=True)
    total_sales = sales.groupBy("product").agg(sum("amount").alias("total_sales"))
    total_sales.show()

    spark.stop()
