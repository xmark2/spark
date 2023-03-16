# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/part-00000

# COMMAND ----------

airlinesDF = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema","true") \
.option("samplingRatio", "0.0001") \
.load("/databricks-datasets/airlines/part-00000")

# COMMAND ----------

from pyspark.sql.functions import *
airlinesDF.select(column("Origin"), col("Dest"), "Distance").show(10)

# COMMAND ----------

airlinesDF.select("Origin", "Dest", "Distance", "Year","Month","DayofMonth").show(10)

# COMMAND ----------

airlinesDF.selectExpr("Origin", "Dest", "Distance", "to_date(concat(Year,Month,DayofMonth),'yyyyMMdd') as FlightDate").show(10)

# COMMAND ----------

airlinesDF.select("Origin", "Dest", "Distance", expr("to_date(concat(Year,Month,DayofMonth),'yyyyMMdd') as FlightDate")).show(10)

# COMMAND ----------

airlinesDF.select("Origin", "Dest", "Distance", to_date(concat("Year","Month","DayofMonth"),"yyyyMMdd").alias("FlightDate")).show(10)

# COMMAND ----------


