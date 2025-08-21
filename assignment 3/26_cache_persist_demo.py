from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CachePersistDemo").getOrCreate()
    df = spark.read.csv("sales.csv", header=True, inferSchema=True)

  
    t0 = time.time(); df.count(); t1 = time.time()
    print("Cold count time:", t1 - t0)

    # cache and run again
    df_cached = df.cache()
    t0 = time.time(); df_cached.count(); t1 = time.time()
    print("Cached count time:", t1 - t0)

    # persist with MEMORY_AND_DISK
    df.persist(StorageLevel.MEMORY_AND_DISK)
    t0 = time.time(); df.count(); t1 = time.time()
    print("Persisted count time:", t1 - t0)

    spark.stop()
