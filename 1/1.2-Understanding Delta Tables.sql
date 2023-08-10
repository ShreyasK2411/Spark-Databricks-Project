-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## create new table

-- COMMAND ----------

CREATE TABLE employees(
  id INT,
  name STRING,
  salary DOUBLE
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## inserting the records

-- COMMAND ----------

INSERT INTO employees VALUES 
(1,"SHREYAS",90000.00),
(6,"SHREEYANS",60000.00),
(3,"SHILPA",70000.00);

-- COMMAND ----------

SELECT * FROM employees ORDER BY salary;

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/employees')

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## updating the records

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name like '%S';

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### old parquet file is not updated, instead new file is created

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metadata of the table

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## See the table history

-- COMMAND ----------

DESCRIBE HISTORY employees 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## checking the delta logs

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/user/hive/warehouse/employees/_delta_log/
