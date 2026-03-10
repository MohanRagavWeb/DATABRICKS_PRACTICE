from pyspark.sql.functions import col
import dlt

# Bronze layer (Extract)
@dlt.table(
    name="sales_bronze",
    comment="Raw sales data from CSV source"
)
def sales_bronze():

    df = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("/Volumes/workspace/default/my_volume_2/etl_source/etl_sales.csv")

    return df


# Silver layer (Transform)
@dlt.table(
    name="sales_silver",
    comment="Cleaned sales data with calculated total"
)
def sales_silver():

    df = dlt.read("sales_bronze")

    df2 = df.withColumn(
        "total_amount",
        col("price") * col("quantity")
    )

    return df2


# Gold layer (Business layer)
@dlt.table(
    name="sales_gold",
    comment="High value sales"
)
def sales_gold():

    df = dlt.read("sales_silver")

    return df.filter(col("total_amount") > 30000)