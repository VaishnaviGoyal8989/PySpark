from pyspark import SparkContext


sc = SparkContext("local", "WordCount")
rdd = sc.parallelize(["hello world", "hello spark", "hello python"])

word_counts = (
    rdd.flatMap(lambda line: line.split())
       .map(lambda word: (word, 1))
       .reduceByKey(lambda a, b: a + b)
       .sortBy(lambda x: x[1], ascending=False)
)

print("=== Word Count ===")
for word, count in word_counts.collect():
    print(f"{word}: {count}")

sc.stop()
