from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SortDescending").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df.orderBy(df["salary"].desc()).show()

    spark.stop()
