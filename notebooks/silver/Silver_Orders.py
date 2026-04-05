# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@retaildatalake26.dfs.core.windows.net/orders_clean")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data", "rescued_data")


# COMMAND ----------

df = df.drop("rescued_data")
display(df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

df = df.withColumn("order_date", to_timestamp(col("order_date")))
df.display()

# COMMAND ----------

from pyspark.sql.functions import year
df = df.withColumn("year", year(col("order_date")))

df.display()

# COMMAND ----------

df1 = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

df1.display()

# COMMAND ----------

df1 = df1.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()


# COMMAND ----------

df1 = df1.withColumn("row_flag",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

class windows:

  def dense_rank(self,df):

    df_dense_rank = df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

    return df_dense_rank

  def rank(self,df):

    df_rank = df.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

    return df_rank

  def row_number(self,df):  

    df_row_number = df.withColumn("row_flag",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

    return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj = windows()  

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@retaildatalake26.dfs.core.windows.net/orders_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS `retails-catalog`.silver.orders_silver
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://silver@retaildatalake26.dfs.core.windows.net/orders_clean'