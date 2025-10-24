# Databricks notebook source
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.bronze_mte.ComunidadesOcio")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Transformación de columna NivelMadurez string a integer
df = df.withColumn("NivelMadurez", df["NivelMadurez"].cast(IntegerType()))

# Se decide mantener las celdas vacias como NULL

# COMMAND ----------

df.display()

# COMMAND ----------

# Save to SILVER catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.ComunidadesOcio')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.ComunidadesOcio')