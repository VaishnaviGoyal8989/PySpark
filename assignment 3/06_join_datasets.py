from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

    df1 = spark.read.csv("employees.csv", header=True, inferSchema=True)
    df2 = spark.read.csv("departments.csv", header=True, inferSchema=True)

    joined_df = df1.join(df2, df1["dept_id"] == df2["dept_id"], "inner")
    joined_df.show()

    spark.stop()
