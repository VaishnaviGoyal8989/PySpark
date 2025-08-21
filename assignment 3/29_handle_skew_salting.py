from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, concat, lit, col
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SkewHandlingSalting").getOrCreate()
    df = spark.read.csv("sales.csv", header=True, inferSchema=True)

    # assume product is skewed
    salted = df.withColumn("salt", (F.floor(rand() * 5)).cast("int"))
    salted = salted.withColumn("salted_key", concat(col("product"), lit("_"), col("salt")))

    # aggregate on salted key
    agg_salted = salted.groupBy("salted_key").agg(F.sum("amount").alias("sum_amount"))
    # split salted_key back to product
    agg_back = agg_salted.withColumn("product", F.split(col("salted_key"), "_").getItem(0))
    result = agg_back.groupBy("product").agg(F.sum("sum_amount").alias("total_amount"))
    result.show()
    spark.stop()
