from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("ECommerceSalesAnalysis") \
    .getOrCreate()

# 2. Load CSV datasets
orders_df = spark.read.option("header", True).option("inferSchema", True).csv("Orders.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("Users.csv")

# 3. Calculate total sales per product
orders_df = orders_df.withColumn("total_price", col("quantity") * col("price"))
total_sales_product = orders_df.groupBy("product_id").agg(_sum("total_price").alias("total_revenue"))
print("Total Sales per Product:")
total_sales_product.show()

# Top 10 products by revenue
top10_products = total_sales_product.orderBy(col("total_revenue").desc()).limit(10)
print("Top 10 Products by Revenue:")
top10_products.show()

# 4. Top 5 locations by revenue
orders_users_df = orders_df.join(users_df, on="user_id", how="inner")
top5_locations = orders_users_df.groupBy("location").agg(_sum("total_price").alias("location_revenue")) \
                                .orderBy(col("location_revenue").desc()).limit(5)
print("Top 5 Locations by Revenue:")
top5_locations.show()

# 5. Identify repeat customers
repeat_customers = orders_df.groupBy("user_id").agg(count("order_id").alias("order_count")) \
                            .filter(col("order_count") > 5)
repeat_customers_with_names = repeat_customers.join(users_df, on="user_id", how="inner")
print("Repeat Customers (>5 orders):")
repeat_customers_with_names.show()

# 6. Save aggregated results in Parquet format
total_sales_product.write.mode("overwrite").parquet("total_sales_per_product.parquet")
top5_locations.write.mode("overwrite").parquet("top5_locations.parquet")
repeat_customers_with_names.write.mode("overwrite").parquet("repeat_customers.parquet")

print("Aggregated results saved as Parquet files successfully!")

# Stop Spark
spark.stop()
