import dlt
from pyspark.sql.functions import *

# ---------------------------
# 🟤 BRONZE (AUTO LOADER)
# ---------------------------
@dlt.table(
    name="bronze_sales_auto",
    comment="Raw data using Auto Loader"
)
def bronze_sales_auto():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .load("/Volumes/workspace/default/auto_loader/auto_loader_Demo/")


# ---------------------------
# ⚪ SILVER (CLEAN + EXPECT)
# ---------------------------
@dlt.table(
    name="silver_sales_auto"
)
@dlt.expect("valid_amount", "amount IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_sales_auto():
    df = dlt.read_stream("bronze_sales_auto")

    return df.select(
        col("order_id").cast("int"),
        col("customer_name"),
        col("city"),
        col("amount").cast("double"),
        col("order_date").cast("date")
    )


# ---------------------------
# 🟡 GOLD (AGGREGATION)
# ---------------------------
@dlt.table(
    name="gold_sales_auto"
)
def gold_sales_auto():
    df = dlt.read_stream("silver_sales_auto")

    return df.groupBy("city") \
        .agg(sum("amount").alias("total_sales"))