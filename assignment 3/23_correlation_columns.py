from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ColumnCorrelation").getOrCreate()
    df = spark.read.csv("data.csv", header=True, inferSchema=True)

    corr_val = df.stat.corr("salary", "id")  # replace with columns you need
    print(f"Correlation(salary, id) = {corr_val}")
    spark.stop()
