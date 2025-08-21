from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Top5Words").getOrCreate()
    sc = spark.sparkContext

    text = sc.textFile("input.txt")
    word_counts = (text.flatMap(lambda line: line.split())
                        .map(lambda word: (word, 1))
                        .reduceByKey(lambda a, b: a + b)
                        .sortBy(lambda x: -x[1]))
    print(word_counts.take(5))

    spark.stop()
