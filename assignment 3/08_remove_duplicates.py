from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df_no_duplicates = df.dropDuplicates()
    df_no_duplicates.show()

    spark.stop()
