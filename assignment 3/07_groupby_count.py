from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GroupByCount").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    grouped = df.groupBy("department").count()
    grouped.show()

    spark.stop()
