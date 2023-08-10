-- Databricks notebook source
-- MAGIC %run ../../Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

select profile:first_name,profile:last_name,profile:gender from customers

-- COMMAND ----------

select profile from customers limit 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers_view AS
select customer_id, from_json(profile, schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) as struct_prof from customers;

SELECT * FROM parsed_customers_view;

-- COMMAND ----------

DESCRIBE parsed_customers_view

-- COMMAND ----------

select struct_prof.first_name, struct_prof.last_name from parsed_customers_view

-- COMMAND ----------

-- struct.* flattens the column having struct type
select customer_id,struct_prof.* from parsed_customers_view

-- COMMAND ----------


