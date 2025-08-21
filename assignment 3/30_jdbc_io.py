from pyspark.sql import SparkSession

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("PostgresDBConnection") \
    .config("spark.jars", r"C:\Users\lucky\.m2\repository\org\postgresql\postgresql\42.7.5\postgresql-42.7.5.jar") \
    .getOrCreate()

# 2. Database connection properties
db_url = "jdbc:postgresql://localhost:5432/assignment_db"
db_properties = {
    "user": "postgres",       
    "password": "1234",       
    "driver": "org.postgresql.Driver"
}

# 3. Read data from PostgreSQL
df = spark.read.jdbc(url=db_url, table="expenses_test", properties=db_properties)

print("Data from PostgreSQL table:")
df.show()

# 4. Write data back 
df.write.jdbc(url=db_url, table="expenses_test_copy", mode="overwrite", properties=db_properties)

print("Data written to expenses_test_copy successfully!")

# Stop Spark
spark.stop()
