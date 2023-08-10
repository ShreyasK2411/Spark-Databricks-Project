# Databricks notebook source
# MAGIC %run ../../Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream.
 table("books_csv").
 createOrReplaceTempView("books_streaming_tmp_vw")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw
# MAGIC AS (
# MAGIC SELECT author,count(book_id) as total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author
# MAGIC )

# COMMAND ----------

# MAGIC %sql select * from author_counts_tmp_vw

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
 .writeStream
 .trigger(processingTime='4 seconds')
 .outputMode("complete")
 .option("checkpointLocation","dbfs:/mnt/Shared/author_counts_checkpoint")
 .table("author_counts")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books_csv
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books_csv
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
 .writeStream
 .trigger(availableNow=True)
 .outputMode("complete")
 .option("checkpointLocation","dbfs:/mnt/Shared/author_counts_checkpoint")
 .table("author_counts")
 .awaitTermination()
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts
