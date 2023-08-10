-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones (
  id int,
  name STRING,
  brand STRING,
  year int
);

INSERT INTO smartphones
VALUES 
(1,'iPhone14','Apple',2022),
(9,'iPhone15','Apple',2023),
(7,'GalaxyS22','Samsung',2024),
(5,'GalaxyS9','Samsung',2025),
(2,'Redmi 3S Prime','Xiaomi',2026),
(3,'Redmi 5s Prime','Xiaomi',2027);

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE VIEW view_samsung_phones
AS
SELECT * FROM smartphones
WHERE brand='Samsung';

-- COMMAND ----------

SELECT * FROM view_samsung_phones;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # creating a temporary views

-- COMMAND ----------

CREATE TEMP VIEW tmp_view_brands
AS
SELECT DISTINCT brand FROM smartphones;

-- COMMAND ----------

SELECT * FROM tmp_view_brands;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating global temp view

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW gb_tmp_view_latest_phones
AS
SELECT * FROM smartphones
WHERE year > 2023;

-- COMMAND ----------

SELECT * FROM global_temp.gb_tmp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES in global_temp
