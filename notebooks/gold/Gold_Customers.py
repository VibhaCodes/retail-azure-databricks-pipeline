# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Data Reading From Source"

# COMMAND ----------

df = spark.sql("select * from `retails-catalog`.silver.customers_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing Duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])


# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dividing New vs Old Records**

# COMMAND ----------

dbutils.widgets.text("init_load_flag", "0")
init_load_flag = dbutils.widgets.get("init_load_flag")

# COMMAND ----------

if init_load_flag == 0:

    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date 
                       from `retails-catalog`.gold.DimCustomers''')
    
else: 

    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date 
                        FROM `retails-catalog`.silver.customers_silver where 1=0''')

# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Key - All The Values**

# COMMAND ----------

df = df.withColumn("DimCustomerKey", monotonically_increasing_id()+ lit(1))


# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

if init_load_flag == 0:

    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date 
                       from `retails-catalog`.gold.DimCustomers''')
    
else: 

    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date 
                        FROM `retails-catalog`.silver.customers_silver where 1=0''')

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reanming Columns of df_old**

# COMMAND ----------

print(df_old.columns)

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
                    .withColumnRenamed("customer_id", "old_customer_id")\
                    .withColumnRenamed("create_date", "old_create_date")\
                    .withColumnRenamed("update_date", "old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Join with the Old Records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Separating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())
df_new.display()

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())
df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

df_old = df_old.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# first remove new-side key, because old key preserve karni hai
df_old = df_old.drop("DimCustomerKey")

# remove unnecessary helper columns
df_old = df_old.drop("old_customer_id", "old_update_date")

# old key ko final key banao
df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# old create_date ko final create_date banao
df_old = df_old.withColumnRenamed("old_create_date", "create_date")

# update_date refresh
df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df New**

# COMMAND ----------

df_new.display()

# COMMAND ----------

# Dropping all the columns which are not required

df_new = df_new.drop('old_DimCustomerKey', 'old_customer_id','old_update_date','old_create_date')

# Recreating "update_date", "current_date" columns with current timestamp

df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1)) 

# COMMAND ----------

df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ****Adding Max Surrogate Key****

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0

else: 
  df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from `retails-catalog`.gold.DimCustomers")

  #Converting df_maxsur to max_surrogate_key
  max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']


# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

print("df_new:", df_new.columns)
print("df_old:", df_old.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ****Union of df_old and df_new****

# COMMAND ----------

from pyspark.sql.functions import col

df_new = df_new.withColumn("DimCustomerKey", col("DimCustomerKey").cast("bigint")) \
               .withColumn("create_date", col("create_date").cast("timestamp")) \
               .withColumn("update_date", col("update_date").cast("timestamp"))

df_old = df_old.withColumn("DimCustomerKey", col("DimCustomerKey").cast("bigint")) \
               .withColumn("create_date", col("create_date").cast("timestamp")) \
               .withColumn("update_date", col("update_date").cast("timestamp"))

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## **SCD Type - 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark.catalog.tableExists("`retails-catalog`.gold.DimCustomers")):
    
    dlt_obj = DeltaTable.forPath(spark,"abfss://gold@retaildatalake26.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path","abfss://gold@retaildatalake26.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("`retails-catalog`.gold.DimCustomers")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `retails-catalog`.gold.dimcustomers