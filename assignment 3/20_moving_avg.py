from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovingAverage").getOrCreate()

    sales = spark.read.csv("sales.csv", header=True, inferSchema=True)
    windowSpec = Window.orderBy("date").rowsBetween(-2, 0)  # moving window of 3
    sales.withColumn("moving_avg", avg("amount").over(windowSpec)).show()

    spark.stop()
