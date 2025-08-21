from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    spark = SparkSession.builder.appName("UnpivotDataset").getOrCreate()

    df = spark.read.csv("wide_data.csv", header=True, inferSchema=True)
    unpivoted = df.selectExpr("id", "stack(3, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3) as (quarter, value)")
    unpivoted.show()

    spark.stop()
