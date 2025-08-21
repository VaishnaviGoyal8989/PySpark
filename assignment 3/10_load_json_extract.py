from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LoadJSONExtract").getOrCreate()

    df = spark.read.json("data.json")
    df.select("id", "name").show()

    spark.stop()
