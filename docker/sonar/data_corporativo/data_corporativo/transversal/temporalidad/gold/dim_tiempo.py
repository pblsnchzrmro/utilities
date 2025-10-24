# Databricks notebook source
# Read table from catalog
df = spark.read.table("transversal.silver_temporalidad.dim_tiempo")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('transversal.gold_temporalidad.dim_tiempo')