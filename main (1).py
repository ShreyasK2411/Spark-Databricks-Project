# Databricks notebook source
# MAGIC %md
# MAGIC # Magic Commands

# COMMAND ----------

# MAGIC %run /includes/setup

# COMMAND ----------

print(full_name) # variable exported by running above notebook

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

dbutils.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

display(files)
