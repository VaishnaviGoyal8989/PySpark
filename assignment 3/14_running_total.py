from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RunningTotal").getOrCreate()

    sales = spark.read.csv("sales.csv", header=True, inferSchema=True)
    windowSpec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    sales.withColumn("running_total", sum("amount").over(windowSpec)).show()

    spark.stop()
