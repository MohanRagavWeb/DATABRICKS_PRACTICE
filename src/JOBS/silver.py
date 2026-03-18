from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.getOrCreate()

df = spark.table("bronze_sales_job")

df_clean = df \
    .withColumn("amount", col("amount").cast("int")) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .dropDuplicates()

df_clean.write.mode("overwrite").saveAsTable("silver_sales_job")

print("✅ Silver table created")