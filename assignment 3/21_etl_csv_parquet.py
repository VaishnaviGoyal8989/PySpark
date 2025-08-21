from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL_CSV_to_Parquet").getOrCreate()
    df = spark.read.csv("sales.csv", header=True, inferSchema=True)

    # transform: filter, cast, add column
    df_transformed = (df.filter(col("amount") > 0)
                        .withColumn("amount", col("amount").cast("double")))

    # write to parquet
    df_transformed.write.mode("overwrite").parquet("output/parquet_sales")
    spark.stop()
