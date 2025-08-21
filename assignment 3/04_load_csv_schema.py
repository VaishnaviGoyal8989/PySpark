from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LoadCSVSchema").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df.printSchema()

    spark.stop()
