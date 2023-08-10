# Databricks notebook source
# MAGIC %run ../../Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f'{dataset_bookstore}/orders-raw')
display(files)

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","parquet")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_tmp AS
# MAGIC SELECT *, current_timestamp() as arrival_time, input_file_name() source_file
# MAGIC FROM orders_raw_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_tmp

# COMMAND ----------

(spark.table("orders_tmp")
 .writeStream
 .format("delta")
 .option("checkpointLocation","dbfs:/mnt/demo/checkpoints/orders_brnz_ckpt")
 .outputMode("append")
 .table("orders_bronze")
 )

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # ------------Silver Layer------------

# COMMAND ----------

(spark.read
 .format("json")
 .load(f"{dataset_bookstore}/customers-json")
 .createOrReplaceTempView("customers_lookup")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup

# COMMAND ----------

(spark.readStream
 .table("orders_bronze")
 .createOrReplaceTempView("orders_bronze_tmp")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
 .writeStream
 .format("delta")
 .option("checkpointLocation","dbfs:/mnt/demo/checkpoints/orders_slvr_ckpt")
 .outputMode("append")
 .table("orders_silver")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # ----------------Gold layer----------------

# COMMAND ----------

(
spark.readStream
.table("orders_silver")
.createOrReplaceTempView("orders_silver_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

# stopping all active streams
for stream in spark.streams.active:
    print("Stopping Stream: "+stream.id)
    stream.stop()
    stream.awaitTermination()
