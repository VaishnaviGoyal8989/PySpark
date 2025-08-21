from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FilterLines").getOrCreate()
    sc = spark.sparkContext

    keyword = "Spark"
    text_file = sc.textFile("input.txt")
    filtered = text_file.filter(lambda line: keyword in line)

    filtered.saveAsTextFile("output_filter")
    spark.stop()
