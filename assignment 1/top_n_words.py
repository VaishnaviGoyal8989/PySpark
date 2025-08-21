from pyspark import SparkContext

sc = SparkContext("local", "TopNWords")

rdd = sc.parallelize(["hello world", "hello spark", "hello python"])

N = 2  

top_n = (
    rdd.flatMap(lambda line: line.split())
       .map(lambda word: (word, 1))
       .reduceByKey(lambda a, b: a + b)
       .sortBy(lambda x: x[1], ascending=False)
       .take(N)
)

print(f"=== Top {N} Most Frequent Words ===")
for word, count in top_n:
    print(f"{word}: {count}")

sc.stop()
