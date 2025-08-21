from pyspark.sql import SparkSession
from itertools import combinations

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCoOccurrence").getOrCreate()
    sc = spark.sparkContext

    text = sc.textFile("input.txt")
    pairs = (text.map(lambda line: line.split())
                 .flatMap(lambda words: combinations(set(words), 2))
                 .map(lambda pair: (pair, 1))
                 .reduceByKey(lambda a, b: a + b))

    pairs.saveAsTextFile("output_cooccurrence")
    spark.stop()
