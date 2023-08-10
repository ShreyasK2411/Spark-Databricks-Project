-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## creating a managed table

-- COMMAND ----------

CREATE TABLE managed_default
(width int,length int, height int);

INSERT INTO managed_default
VALUES (1,2,3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metadata of extended tables

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating external table

-- COMMAND ----------

CREATE TABLE external_default
(width int,length int, height int)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_default
VALUES (1,2,3);

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating tables in a database

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
(width int,length int, height int);

INSERT INTO managed_new_default
VALUES (1,2,3);
----------------------------------------------------------
CREATE TABLE external_new_default
(width int,length int, height int)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_new_default
VALUES (1,2,3);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating a external schema

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom_default
(width int,length int, height int);

INSERT INTO managed_custom_default
VALUES (1,2,3);
----------------------------------------------------------
CREATE TABLE external_custom_default
(width int,length int, height int)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_custom_default
VALUES (1,2,3);

-- COMMAND ----------

DROP TABLE external_custom_default;
DROP TABLE managed_custom_default;

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC ls 'dbfs:/Shared/schemas/custom.db/external_custom_default'
