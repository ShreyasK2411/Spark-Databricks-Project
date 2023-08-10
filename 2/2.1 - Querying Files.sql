-- Databricks notebook source
-- MAGIC %run ../../Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f'{dataset_bookstore}/customers-json')
-- MAGIC display(files)

-- COMMAND ----------

SELECT * from JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`

-- COMMAND ----------

SELECT count(*) from JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

SELECT *,input_file_name() source_file from JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`

-- COMMAND ----------

SELECT * from text.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

SELECT * from binaryFile.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

SELECT * from csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv 
(book_id STRING,title STRING,author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header="true",
  delimiter=";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.
-- MAGIC  table('books_csv').
-- MAGIC  write.
-- MAGIC  mode("append").
-- MAGIC  format("csv").
-- MAGIC  option("header","true").
-- MAGIC  option("delimiter",";").
-- MAGIC  save(f"{dataset_bookstore}/books_csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f'{dataset_bookstore}/books_csv')
-- MAGIC display(files)

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

select count(*) from books_csv
