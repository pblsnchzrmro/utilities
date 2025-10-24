# Databricks notebook source
df_formaciones = spark.read.table("people.silver_formaciones.formaciones_api")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_formaciones = df_formaciones.withColumn("FechaCarga", current_timestamp())

df_formaciones = df_formaciones.withColumn("horas", df_formaciones["horas"].cast("int"))
df_formaciones = df_formaciones.withColumn("fechaInicio", df_formaciones["fechaInicio"].cast("date")).withColumn("fechaFin", df_formaciones["fechaFin"].cast("date"))

df_formaciones = df_formaciones.withColumnRenamed("programa", "Programa").withColumnRenamed("curso", "Curso").withColumnRenamed("modalidad", "Modalidad").withColumnRenamed("horas", "Horas").withColumnRenamed("proveedor", "Proveedor").withColumnRenamed("fechaInicio", "FechaInicio").withColumnRenamed("fechaFin", "FechaFin").withColumnRenamed("nombre", "Nombre").withColumnRenamed("email", "Email").withColumnRenamed("empresa", "Empresa").withColumnRenamed("hgz", "Hgz").withColumnRenamed("hgr", "Hgr")


# COMMAND ----------

df_formaciones.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_formaciones.formaciones')