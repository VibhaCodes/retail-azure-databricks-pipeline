# Databricks notebook source
# MAGIC %md
# MAGIC ### Dynamic Capabilities

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

p_file_name

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://rawdataset@retaildatalake26.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"abfss://bronze@retaildatalake26.dfs.core.windows.net/checkpoint_{p_file_name}_clean") \
    .load(f"abfss://rawdataset@retaildatalake26.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

q = df.writeStream.format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", f"abfss://bronze@retaildatalake26.dfs.core.windows.net/checkpoint_{p_file_name}_clean") \
    .option("path", f"abfss://bronze@retaildatalake26.dfs.core.windows.net/{p_file_name}_clean") \
    .trigger(once=True) \
    .start()

q.awaitTermination()
print(f"{p_file_name} loaded successfully")

# COMMAND ----------

df_check = spark.read.format("parquet") \
    .load(f"abfss://bronze@retaildatalake26.dfs.core.windows.net/{p_file_name}_clean")

display(df_check)

# COMMAND ----------

