# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.bronze_mte.SubhBU")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Transformación de columnas booleanas en enteros 1/0
df = df.withColumn("Activo", when(col("Activo") == True, 1).otherwise(0))
df = df.withColumn("ActivoUnhiberse", when(col("ActivoUnhiberse") == True, 1).otherwise(0))

# Se decide mantener las celdas vacias como NULL
df = df.withColumn("Responsable", when(col("Responsable") == "", None).otherwise(col("Responsable")))
df = df.withColumn("ResponsableEmail", when(col("ResponsableEmail") == "", None).otherwise(col("ResponsableEmail")))

df = df.withColumn("ResponsableAdjunto", when(col("ResponsableAdjunto") == "", None).otherwise(col("ResponsableAdjunto")))
df = df.withColumn("ResponsableAdjuntoEmail", when(col("ResponsableAdjuntoEmail") == "", None).otherwise(col("ResponsableAdjuntoEmail")))

df = df.withColumn("Aprobador", when(col("Aprobador") == "", None).otherwise(col("Aprobador")))
df = df.withColumn("AprobadorEmail", when(col("AprobadorEmail") == "", None).otherwise(col("AprobadorEmail")))

df = df.withColumn("hBU", when(col("hBU") == "", None).otherwise(col("hBU")))

# COMMAND ----------

df.display()

# COMMAND ----------

# Save to SILVER catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.SubhBU')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.SubhBU')