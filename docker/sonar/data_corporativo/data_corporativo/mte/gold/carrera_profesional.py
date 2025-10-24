# Databricks notebook source
df = spark.read.table("operaciones.silver_mte.carreraprofesional")

df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = df.withColumn("FechaCarga", current_timestamp())

df = df.orderBy("IdCarreraProfesional")

df.display()

# COMMAND ----------

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.dim_carreraprofesional')

# df.write.mode("append").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operacional.gold_mte.Mercado_HIST')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.dim_carreraprofesional')