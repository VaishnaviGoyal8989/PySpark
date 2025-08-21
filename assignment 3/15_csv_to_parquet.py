from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df.write.parquet("output_parquet", mode="overwrite")

    spark.stop()
