// Databricks notebook source
// MAGIC %md
// MAGIC # Friends by Age

// COMMAND ----------

case class Person(id:Int, name:String, age:Int, friendS:Int)

// COMMAND ----------

val rawData = spark.read
.format("csv")
.option("header","true")
.option("inferSchema","true")
.load("dbfs:///FileStore/tables/fakefriends.csv")
.as[Person]

// COMMAND ----------

rawData.printSchema()

// COMMAND ----------

rawData.createOrReplaceTempView("people")

// COMMAND ----------

val finalData = spark.sql("SELECT age,round(mean(friends),2) as total_friends FROM people GROUP BY age ORDER BY age")

// COMMAND ----------

finalData.printSchema()

// COMMAND ----------

finalData.show()

// COMMAND ----------

finalData.collect().foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC # Word Count using spark SQL

// COMMAND ----------

case class Book(value:String)

// COMMAND ----------

val txtData = spark.read
.text("dbfs:///FileStore/tables/book.txt")
.as[Book]

// COMMAND ----------

txtData.createOrReplaceTempView("text")

// COMMAND ----------

spark.sql("SELECT word, count(word) as `count` FROM (SELECT explode(split(lower(value),' ')) as word FROM text) GROUP BY word ORDER BY `count`").show(50)

// COMMAND ----------

// MAGIC %md
// MAGIC # Most Popular Superhero

// COMMAND ----------

import org.apache.spark.sql.functions.{split,col,size,sum}

// COMMAND ----------

case class Superhero(value:String)

// COMMAND ----------

val idConnections = spark.read
.text("dbfs:///FileStore/tables/Marvel_graph.txt")
.as[Superhero]

// COMMAND ----------

idConnections.createOrReplaceTempView("marvel_connections")

// COMMAND ----------

val connections = idConnections
.withColumn("id",split(col("value")," ")(0))
.withColumn("connections",size(split(col("value")," "))-1)

// COMMAND ----------

val popular = connections.groupBy("id")
.agg(
  sum("connections")
  .alias("connections")
  )
.sort($"connections".desc)
.first()

// COMMAND ----------

case class SuperHeroNames(id:Int,name:String)

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,IntegerType,StringType}

// COMMAND ----------

val superHeroNameSchema = new StructType()
.add("id", IntegerType, nullable=true)
.add("name", StringType, nullable=true)

// COMMAND ----------

val names = spark.read
.schema(superHeroNameSchema)
.option("sep"," ")
.csv("dbfs:///FileStore/tables/Marvel_names.txt")
.as[SuperHeroNames]

// COMMAND ----------

names.createOrReplaceTempView("superhero_names")

// COMMAND ----------

names
.filter($"id" === popular(0))
.select("name")
.show(truncate=false)

// COMMAND ----------

spark.sql("SELECT b.name FROM (SELECT id,sum(connections) as total_connections FROM (SELECT split(value,' ')[0] as id, size(split(value,' '))-1 as connections FROM marvel_connections) GROUP BY ID ORDER BY total_connections desc LIMIT 1) a INNER JOIN superhero_names b ON a.id=b.id")
.show()

// COMMAND ----------

spark.sql("SELECT id,sum(connections) as total_connections FROM (SELECT split(value,' ')[0] as id, size(split(value,' '))-1 as connections FROM marvel_connections) GROUP BY ID ORDER BY total_connections desc LIMIT 1").show()
