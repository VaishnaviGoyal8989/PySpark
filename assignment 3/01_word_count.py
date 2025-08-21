from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    sc = spark.sparkContext

    text_file = sc.textFile("input.txt")  # replace with your file path
    counts = (text_file.flatMap(lambda line: line.split())
                        .map(lambda word: (word, 1))
                        .reduceByKey(lambda a, b: a + b))

    counts.saveAsTextFile("output_wordcount")
    spark.stop()
