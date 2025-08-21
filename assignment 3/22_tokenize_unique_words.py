from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, trim

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TokenizeUniqueWords").getOrCreate()
    df = spark.read.text("input.txt").toDF("line")

    words = (df.select(explode(split(lower(trim(df.line)), "\\W+")).alias("word"))
               .filter("word != ''")
               .distinct()
               .count())

    print(f"Unique words: {words}")
    spark.stop()
