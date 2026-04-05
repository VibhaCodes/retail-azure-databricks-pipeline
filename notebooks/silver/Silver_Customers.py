# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@retaildatalake26.dfs.core.windows.net/customers_clean")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("domains", split("email", "@")[1])
df.display()


# COMMAND ----------

df = df.drop("_rescued_data")
df.display()


# COMMAND ----------

from pyspark.sql.functions import count

df1 = df.groupBy("domains") \
    .agg(count("customer_id").alias("total_customers")) \
    .sort("total_customers", ascending=False)

display(df1)

# COMMAND ----------

df_gmail = df.filter(col('domains')=="gmail.com")
df_gmail.display()
time.sleep(5)

df_yahoo = df.filter(col('domains')=="yahoo.com")
df_yahoo.display()
time.sleep(5)

df_hotmail = df.filter(col('domains')=="hotmail.com")
df_hotmail.display()
time.sleep(5)

# COMMAND ----------

df = df.withColumn("full_name",concat(col('first_name'),lit(' '),col('last_name')))
df.display()
df = df.drop('first_name','last_name')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@retaildatalake26.dfs.core.windows.net/customers_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists `retails-catalog`.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@retaildatalake26.dfs.core.windows.net/customers_clean'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from `retails-catalog`.silver.customers_silver

# COMMAND ----------

