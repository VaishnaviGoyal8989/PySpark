from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AccumulatorsEvents").getOrCreate()
sc = spark.sparkContext

# Accumulator for null salaries
null_salary_acc = sc.accumulator(0)

data = [("John", 5000), ("Mike", None), ("Sara", 7000), ("Nina", None)]
rdd = sc.parallelize(data)

def check_salary(record):
    global null_salary_acc
    if record[1] is None:
        null_salary_acc += 1
    return record

rdd.foreach(check_salary)

print("Total null salaries:", null_salary_acc.value)

spark.stop()

