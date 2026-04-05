# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@retaildatalake26.dfs.core.windows.net/products_clean")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC #FUNCTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function `retails-catalog`.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, `retails-catalog`.bronze.discount_func(price) as discounted_price
# MAGIC from products

# COMMAND ----------

df = df.withColumn("discounted_price", expr("`retails-catalog`.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function `retails-catalog`.bronze.upper(p_brande string)
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC $$
# MAGIC   return p_brande.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id , brand, `retails-catalog`.bronze.upper(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://silver@retaildatalake26.dfs.core.windows.net/products")\
            .save()
            

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists `retails-catalog`.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@retaildatalake26.dfs.core.windows.net/products'