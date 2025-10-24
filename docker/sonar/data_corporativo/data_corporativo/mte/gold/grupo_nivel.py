# Databricks notebook source
# Read table from catalog
df = spark.read.table("operaciones.silver_mte.GrupoNivel")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = df.withColumn("FechaCarga", current_timestamp())

df = df.orderBy("IdGrupoNivel")

df.display()

# COMMAND ----------

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.dim_gruponivel')

# df.write.mode("append").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operacional.gold_mte.Mercado_HIST')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.dim_gruponivel')

# df.write.mode("append").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.Mercado_HIST')