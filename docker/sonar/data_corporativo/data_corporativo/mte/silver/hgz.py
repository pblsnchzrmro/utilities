# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.bronze_mte.hGZ")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Transformación de columnas booleanas en enteros 1/0
df = df.withColumn("Activo", when(col("Activo") == True, 1).otherwise(0))
df = df.withColumn("ActivoUnhiberse", when(col("ActivoUnhiberse") == True, 1).otherwise(0))

# Se decide mantener las celdas vacias como NULL
df = df.withColumn("Responsable", when(col("Responsable") == "", None).otherwise(col("Responsable")))
df = df.withColumn("ResponsableEmail", when(col("ResponsableEmail") == "", None).otherwise(col("ResponsableEmail")))

# COMMAND ----------

df.display()

# COMMAND ----------

# Save to SILVER catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.hGZ')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.hGZ')