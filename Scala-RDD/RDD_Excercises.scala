// Databricks notebook source
// MAGIC %md
// MAGIC # Minimum Temprature by station

// COMMAND ----------

val raw = sc.textFile("dbfs:///FileStore/tables/1800.csv")

// COMMAND ----------

def parseLines(line:String): (String,String,Float) = {
  val fields = line.split(",")
  val stationId = fields(0)
  val entryType = fields(2)
  val temprature = fields(3).toFloat * 0.1f * (0.9f / 0.5f) + 32.0f
  (stationId,entryType,temprature)
}

// COMMAND ----------

val parsed = raw.map(parseLines)

// COMMAND ----------

val filtered = parsed.filter(x=>x._2=="TMIN")

// COMMAND ----------

val stationTemps = filtered.map(x=>(x._1,x._3.toFloat))

// COMMAND ----------

val minTempsByStation = stationTemps.reduceByKey((x,y)=>math.min(x,y))

// COMMAND ----------

for (results <- minTempsByStation.collect()) {
  println(f"${results._1} min temprature is ${results._2}%.2f F")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Total Amount Spent by Customer

// COMMAND ----------

val raw = sc.textFile("dbfs:///FileStore/tables/customer_orders.csv")

// COMMAND ----------

def parseLines(line:String): (Int,Float) = {
  val fields = line.split(",")
  val customerID = fields(0).toInt
  val amount = fields(2).toFloat
  (customerID,amount)
}

// COMMAND ----------

val customerSpendings = raw.map(parseLines)

// COMMAND ----------

val customerTotalSpending = customerSpendings.reduceByKey((x,y)=>x+y)

// COMMAND ----------

val sortedCustomerTotalSpending = customerTotalSpending.map(x=>(x._2,x._1)).sortByKey()

// COMMAND ----------

sortedCustomerTotalSpending.collect().foreach(println)
