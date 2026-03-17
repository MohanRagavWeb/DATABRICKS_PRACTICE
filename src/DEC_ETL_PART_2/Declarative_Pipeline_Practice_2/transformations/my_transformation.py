import dlt
from pyspark.sql.functions import *

# ---------------------------
# 🟤 BRONZE LAYER (RAW DATA)
# ---------------------------
@dlt.table(
    name="bronze_sales_dec2",
    comment="Raw sales data"
)
def bronze_sales_dec2():
    return spark.read.format("csv") \
        .option("header", "true") \
        .load("/Volumes/workspace/default/declarative/decp_sales_2.csv")


# ---------------------------
# ⚪ SILVER LAYER (CLEANED DATA + EXPECTATIONS)
# ---------------------------
@dlt.table(
    name="silver_sales_dec2",
    comment="Cleaned sales data"
)
@dlt.expect("valid_amount", "amount IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_sales():
    df = dlt.read("bronze_sales_dec2")

    return df.select(
        col("order_id").cast("int"),
        col("customer_name"),
        col("city"),
        col("amount").cast("double"),
        col("order_date").cast("date")
    )


# ---------------------------
# 🟡 GOLD LAYER (AGGREGATION)
# ---------------------------
@dlt.table(
    name="gold_sales_summary_dec2",
    comment="Aggregated sales per city"
)
def gold_sales_summary():
    df = dlt.read("silver_sales_dec2")

    return df.groupBy("city") \
        .agg(sum("amount").alias("total_sales"))