# Databricks notebook source
df_jobs = spark.read.table("people.silver_talentclue.jobs")
df_applicants = spark.read.table("people.silver_talentclue.applicants")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_jobs = df_jobs.withColumn("FechaCarga", current_timestamp())
df_applicants = df_applicants.withColumn("FechaCarga", current_timestamp())

df_jobs = df_jobs.withColumnRenamed("IDPosicion", "IdPosicion")
df_jobs = df_jobs.withColumn("IdPosicion", df_jobs["IdPosicion"].cast("integer"))

df_applicants = df_applicants.withColumnRenamed("IDPosicion", "IdPosicion").withColumnRenamed("IDCandidato", "IdCandidato")
df_applicants = df_applicants.withColumn("IdPosicion", df_applicants["IdPosicion"].cast("integer"))
df_applicants = df_applicants.withColumn("IdCandidato", df_applicants["IdCandidato"].cast("integer"))
df_applicants = df_applicants.withColumn("FechaNacimiento", df_applicants["FechaNacimiento"].cast("date"))


# COMMAND ----------

df_jobs.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_talentclue.jobs')

df_applicants.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_talentclue.applicants')