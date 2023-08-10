-- Databricks notebook source
-- MAGIC %run ../../Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customer_updates AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json-new`; 

MERGE INTO customers c
USING customer_updates u
ON c.customer_id=u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
UPDATE SET email=u.email, updated=u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tmp_books_view 
(book_id STRING,title STRING,author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path="${dataset.bookstore}/books-csv",
  header="true",
  delimiter=";"
);

DROP TABLE books_csv;

CREATE TABLE books_csv AS
SELECT * FROM tmp_books_view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates 
(book_id STRING,title STRING,author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path="${dataset.bookstore}/books-csv-new",
  header="true",
  delimiter=";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books_csv b
USING books_updates u
ON b.book_id=u.book_id AND b.title=u.title
WHEN NOT MATCHED AND u.category='Computer Science' THEN
INSERT *
