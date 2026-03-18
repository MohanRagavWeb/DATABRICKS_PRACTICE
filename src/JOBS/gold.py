from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder.getOrCreate()

df = spark.table("silver_sales_job")

df_agg = df.groupBy("city") \
    .agg(
        sum("amount").alias("total_sales"),
        count("order_id").alias("total_orders")
    )

df_agg.write.mode("overwrite").saveAsTable("gold_sales_job")

print("✅ Gold table created")