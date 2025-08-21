from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("SchemaExamples").getOrCreate()

# Method 1: Inferred Schema
data1 = [("1", "101", 2023, 50000.0), ("2", "101", 2024, 65000.0)]
df1 = spark.createDataFrame(data1, ["id", "org_id", "year", "revenue"])
df1.show()

# Method 2: Explicit Schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("org_id", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("revenue", DoubleType(), True)
])
data2 = [("3", "102", 2023, 30000.0), ("4", "102", 2024, 39000.0)]
df2 = spark.createDataFrame(data2, schema)
df2.show()
