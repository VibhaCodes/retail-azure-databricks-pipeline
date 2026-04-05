# Databricks notebook source
dataset =[
    {
        "file_name" : "orders"
    },
    {
        "file_name" : "customers"
    },
    {
        "file_name" : "products"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", dataset)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC