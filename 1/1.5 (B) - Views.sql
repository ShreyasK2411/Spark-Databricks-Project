-- Databricks notebook source
SHOW TABLES;

-- COMMAND ----------

--global temp views are available till the cluster is alive
SHOW TABLES in global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.gb_tmp_view_latest_phones;
