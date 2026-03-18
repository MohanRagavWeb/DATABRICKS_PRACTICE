from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

file_path = "/Volumes/workspace/default/jobs/sales_jobs.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .load(file_path)

df.write.mode("overwrite").saveAsTable("bronze_sales_job")

print("✅ Bronze table created")