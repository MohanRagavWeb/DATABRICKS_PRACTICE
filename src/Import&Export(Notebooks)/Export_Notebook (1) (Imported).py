# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Analysis Notebook
# MAGIC
# MAGIC This notebook demonstrates basic Databricks operations.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create sample data
# MAGIC 2. Perform transformations
# MAGIC 3. Display results

# COMMAND ----------

data = [
    (1,"Laptop",50000,2),
    (2,"Phone",20000,3),
    (3,"Tablet",15000,1),
    (4,"Headphones",3000,4)
]

columns = ["id","product","price","quantity"]

df = spark.createDataFrame(data, columns)

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn("total_amount", col("price") * col("quantity"))

display(df)

# COMMAND ----------

df.groupBy("product").sum("total_amount").show()
