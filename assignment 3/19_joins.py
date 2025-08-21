from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Joins").getOrCreate()

    df1 = spark.read.csv("employees.csv", header=True, inferSchema=True)
    df2 = spark.read.csv("departments.csv", header=True, inferSchema=True)

    df1.join(df2, "dept_id", "inner").show()
    df1.join(df2, "dept_id", "left").show()
    df1.join(df2, "dept_id", "right").show()

    spark.stop()
