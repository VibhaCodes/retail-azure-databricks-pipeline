# Databricks notebook source
df = spark.read.table("`retails-catalog`.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@retaildatalake26.dfs.core.windows.net/regions")


# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@retaildatalake26.dfs.core.windows.net/regions")
df.display()

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@retaildatalake26.dfs.core.windows.net/customers_clean")
df.display()

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@retaildatalake26.dfs.core.windows.net/orders_clean")
df.display()

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@retaildatalake26.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists `retails-catalog`.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@retaildatalake26.dfs.core.windows.net/regions'