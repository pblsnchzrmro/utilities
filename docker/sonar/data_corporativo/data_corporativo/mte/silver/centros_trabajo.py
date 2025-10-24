# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.bronze_mte.CentrosTrabajo")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Transformación de columnas booleanas en enteros 1/0

# Se decide mantener las celdas vacias como NULL

# COMMAND ----------

df.display()

# COMMAND ----------

# Save to SILVER catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.CentrosTrabajo')

# Save to people catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.CentrosTrabajo')