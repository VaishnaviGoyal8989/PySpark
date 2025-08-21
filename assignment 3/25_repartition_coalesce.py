# 25_repartition_coalesce.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RepartitionCoalesce").getOrCreate()
    df = spark.read.csv("sales.csv", header=True, inferSchema=True)

    # increase partitions for parallelism
    df_repart = df.repartition(8)
    print("Repartition count:", df_repart.rdd.getNumPartitions())

    # reduce partitions before writing small output
    df_coalesced = df_repart.coalesce(2)
    print("Coalesced count:", df_coalesced.rdd.getNumPartitions())

    df_coalesced.write.mode("overwrite").csv("output/repartition_demo")
    spark.stop()
