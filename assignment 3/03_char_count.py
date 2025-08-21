from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CharCount").getOrCreate()
    sc = spark.sparkContext

    text_file = sc.textFile("input.txt")
    char_counts = (text_file.flatMap(lambda line: list(line.replace(" ", "")))
                             .map(lambda ch: (ch, 1))
                             .reduceByKey(lambda a, b: a + b))

    char_counts.saveAsTextFile("output_charcount")
    spark.stop()
