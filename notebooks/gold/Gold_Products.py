# Databricks notebook source
# MAGIC %md
# MAGIC **DLT PIPELINE**

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Table**

# COMMAND ----------

import dlt
from pyspark.sql.functions import * 

# COMMAND ----------

# -------------------------------
# Data quality rules
# -------------------------------
my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# COMMAND ----------

# -------------------------------
# Stage table
# Reads streaming data from Silver
# Drops rows failing expectations
# -------------------------------
@dp.table(name="DimProducts_stage")
@dp.expect_all_or_drop(my_rules)
def DimProducts_stage():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .format("delta").load("`retails-catalog`.silver.products_silver")
    )


# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage(): 

  df = spark.readStream \
        .option("skipChangeCommits","true") \
        .table("`retails-catalog`.silver.products_silver")

  return df

# COMMAND ----------

@dlt.view()
def DimProducts_view():
    return spark.readStream.table("DimProducts_stage")

dlt.create_streaming_table("DimProducts")

dlt.apply_changes(
    target="DimProducts",
    source="DimProducts_view",
    keys=["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)

# COMMAND ----------



# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col

# -------------------------------
# Data quality rules
# -------------------------------
my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# -------------------------------
# Stage table
# Reads streaming data from Silver
# Drops rows failing expectations
# -------------------------------
@dp.table(name="DimProducts_stage")
@dp.expect_all_or_drop(my_rules)
def DimProducts_stage():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("`retails-catalog`.silver.products_silver")
    )

# -------------------------------
# Temporary view
# Intermediate logical layer
# -------------------------------
@dp.temporary_view(name="DimProducts_view")
def DimProducts_view():
    return spark.readStream.table("DimProducts_stage")

# -------------------------------
# Final target streaming table
# Required before applying CDC changes
# -------------------------------
dp.create_streaming_table("DimProducts")

# -------------------------------
# Apply SCD Type 2 changes
# IMPORTANT:
# Replace SOURCE_ORDER_COL with your real
# timestamp / sequence column from source
# Example: updated_at, modified_date, load_time
# -------------------------------
dp.create_auto_cdc_flow(
    target="DimProducts",
    source="DimProducts_view",
    keys=["product_id"],
    sequence_by=col("SOURCE_ORDER_COL"),
    stored_as_scd_type=2
)

# COMMAND ----------

from pyspark import pipelines as dp

@dp.table()
def test_table():    return spark.range(5)

return spark.range(5)