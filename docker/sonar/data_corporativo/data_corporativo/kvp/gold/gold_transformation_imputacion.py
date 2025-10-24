# Databricks notebook source
import requests
import json
from datetime import datetime, timedelta
import calendar
import time
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS operaciones.gold_kvp")

# COMMAND ----------

# Dataframe de la tabla
imputacion_df = spark.sql("SELECT DISTINCT * FROM operaciones.silver_kvp.imputacion")

# SQL para encontrar columnas completamente nulas
columns_list = imputacion_df.columns
agg_exprs = [F.sum(F.when(F.col(c).isNotNull(), 1).otherwise(0)).alias(c) for c in columns_list]
null_check_df = imputacion_df.agg(*agg_exprs)

# Convertir a pandas para procesamiento más fácil
null_check_pd = null_check_df.toPandas()
null_columns = [col for col in null_check_pd.columns if null_check_pd[col][0] == 0]

print(f"Columns with all null values: {null_columns}")

# Identificar columnas dimX (pero no las dimXNombre)
dim_id_columns = [col for col in imputacion_df.columns if col.startswith("dim") and not col.endswith("Nombre")]
columns_to_drop = list(set(null_columns + dim_id_columns))

print(f"Columns to be dropped: {columns_to_drop}")


# COMMAND ----------

# Dataframe sin columnas null + dimX
fact_imputacion_df = imputacion_df.drop(*columns_to_drop)

# Apellido2 esta vacio, que no null
fact_imputacion_df = fact_imputacion_df.drop("apellido2")

# Dataframe nuevo por naming
renamed_df = fact_imputacion_df

# INITCAP()
for old_col in fact_imputacion_df.columns:
    new_col = old_col[0].upper() + old_col[1:]
    renamed_df = renamed_df.withColumnRenamed(old_col, new_col)

# Cast Fecha a un dato tipo fecha
renamed_df = renamed_df.withColumn("Fecha", F.to_date("Fecha"))

# Cast Horas a un dato tipo double
renamed_df = renamed_df.withColumn("Horas", F.col("Horas").cast("double"))

# Cast IdUsuarioMte a un dato tipo int
renamed_df = renamed_df.withColumn("IdUsuarioMte", F.col("IdUsuarioMte").cast("int"))


# Schema final
renamed_df.printSchema()

# Check
renamed_df.show(5)

# COMMAND ----------

df_with_fecha_carga = renamed_df.withColumn("FechaCarga", F.current_timestamp())

# Seleccionamos las columnas finales
df_filtered = df_with_fecha_carga.select(
    F.col("IdProyecto"),
    F.col("IdUsuarioMte").alias("IdUsuario"),  # Renaming 'IdUsuarioMte' to 'IdUsuario'
    F.col("Fecha"),
    F.col("Horas"),
    F.col("Origen"),
    F.col("FechaCarga") 
)

# COMMAND ----------

# Vista
df_filtered.createOrReplaceTempView("fact_imputaciones")

# Guardar como Delta table
spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_kvp.fact_imputaciones 
    AS SELECT DISTINCT * FROM fact_imputaciones
""")

# Verificar tabla contiene datos
spark.sql("SELECT COUNT(*) FROM operaciones.gold_kvp.fact_imputaciones WHERE year(Fecha) = year(current_date()) AND month(Fecha) = month(current_date())").show()