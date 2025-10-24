# Databricks notebook source
# Read table from catalog
df = spark.read.table("operacional.silver_mte.ComunidadesOcio_SILVER")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Column names normalizados

# Selección columnas para vista final Gold
df = df.select('ID','Nombre', 'NivelMadurez', 'Responsable', 'ResponsableEmail', 'FECHA_CARGA')

df.display()

# COMMAND ----------

df = df.withColumnRenamed('ID', 'IdComunidadOcio').withColumnRenamed('Nombre', 'ComunidadOcio')

df.display()

# COMMAND ----------

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operacional.gold_mte.ComunidadesOcio')

df.write.mode("append").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operacional.gold_mte.ComunidadesOcio_HIST')