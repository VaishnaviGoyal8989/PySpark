from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BroadcastLookup").getOrCreate()
    employees = spark.read.csv("employees.csv", header=True, inferSchema=True)
    depts_small = spark.read.csv("departments.csv", header=True, inferSchema=True)  # small table

    # use broadcast for efficient join
    joined = employees.join(broadcast(depts_small), "dept_id", "left")
    joined.show()
    spark.stop()
