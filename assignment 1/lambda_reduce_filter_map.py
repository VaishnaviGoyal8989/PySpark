from pyspark import SparkContext

sc = SparkContext("local", "LambdaReduceFilterMap")

rdd = sc.parallelize(["apple banana", "apple orange", "banana apple", "orange"])

# flatMap + lambda → Split words
words = rdd.flatMap(lambda line: line.split())

# map + lambda → Pair each word with 1
word_pairs = words.map(lambda word: (word, 1))

# reduceByKey + lambda → Count occurrences
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# filter + lambda → Only keep words with count > 1
filtered = word_counts.filter(lambda pair: pair[1] > 1)

print("=== Filtered Word Counts (count > 1) ===")
for word, count in filtered.collect():
    print(f"{word}: {count}")

sc.stop()
