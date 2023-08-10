-- Databricks notebook source
DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * FROM employees VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Another way of accessing versions of table

-- COMMAND ----------

SELECT * FROM employees@v1;

-- COMMAND ----------

DELETE from employees;

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Restoring table to particular version

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 2;

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimizing the data files

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  remove unused old files

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------


