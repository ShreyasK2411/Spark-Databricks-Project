-- Databricks notebook source
-- MAGIC %run ../../Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

SELECT * FROM
(SELECT order_id,books,
FILTER(books, i -> i.quantity >= 2) AS multiple_copies
FROM orders)
WHERE size(multiple_copies) > 0
