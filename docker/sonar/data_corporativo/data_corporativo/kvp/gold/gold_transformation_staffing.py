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
from ast import literal_eval

# COMMAND ----------

# spark.sql("CREATE SCHEMA IF NOT EXISTS operaciones.gold_kvp")

# COMMAND ----------

staffing_df = spark.sql("SELECT DISTINCT * FROM operaciones.silver_kvp.staffing")

# COMMAND ----------

staffing_df.display()

# COMMAND ----------

# Función para evaluar el string como un objeto Python 
def convert_to_list(data_str):
    try:
        return literal_eval(data_str)
    except (ValueError, SyntaxError):
        return []

# UDF (User Defined Function) para aplicar la transformación
convert_to_list_udf = F.udf(convert_to_list, ArrayType(MapType(StringType(), StringType())))

# Aplicar UDF para transformar string en array (para el explode)
kvp_df_with_array = staffing_df.withColumn("dedicacion_array", convert_to_list_udf(F.col("dedicacion")))

# Explode array para tener cada dictionary en una row
exploded_df = kvp_df_with_array.select("*", F.explode("dedicacion_array").alias("dedicacion_item"))

# Paso extra
# Columnas adicionales del df que no son dedicación (por si queremos usarlas en el futuro)
other_columns = [col for col in exploded_df.columns if col not in ["dedicacion_array", "dedicacion_item", "dedicacion"]]

# Dataframe con los campos de dedicación que nos interesan
result_df = exploded_df.select(
    F.col("dedicacion_item.id").cast(IntegerType()).alias("Id"),
    F.col("dedicacion_item.idUsuarioMTE").cast(IntegerType()).alias("IdUsuario"),
    F.col("dedicacion_item.nombre").alias("Nombre"),
    #F.col("dedicacion_item.idProyecto").cast(IntegerType()).alias("IdProyecto"), # IdProyecto viene sin el año
    F.col("dedicacion_item.clave").alias("IdProyecto"),
    F.col("dedicacion_item.proyecto").alias("Proyecto"),
    
    F.to_date(F.col("dedicacion_item.fechaDesde"), "dd/MM/yyyy").alias("FechaInicio"),
    
    F.to_date(F.col("dedicacion_item.fechaHasta"), "dd/MM/yyyy").alias("FechaFin"),
    
    F.col("dedicacion_item.porcentaje").cast(DoubleType()).alias("Porcentaje"),
    F.col("dedicacion_item.puestoTrabajo").alias("PuestoTrabajo"),
    F.col("dedicacion_item.valor").alias("Valor"),
    F.col("dedicacion_item.hbu").alias("Hbu")
)

result_df = result_df.withColumn("Porcentaje", F.col("Porcentaje")/100)
result_df = result_df.withColumn("Nombre", F.initcap("Nombre"))
result_df = result_df.withColumn("Proyecto", F.initcap("Proyecto"))
result_df = result_df.withColumn("IdProyecto", F.regexp_replace(F.col("IdProyecto"), '^"|"$', ''))


# Mostrar resultado
display(result_df)

# COMMAND ----------

result_df_cleaned = result_df.drop("Id", "Nombre", "Proyecto", "PuestoTrabajo", "Valor", "Hbu")
result_df_cleaned = result_df_cleaned.withColumn("FechaCarga", F.current_timestamp())
# cambiar a DateTime las fechas de carga (ver si hay dif entre date y datetime)

# COMMAND ----------

# Vista
result_df_cleaned.createOrReplaceTempView("fact_asignaciones")

# Guardar como Delta table
spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_kvp.fact_asignaciones 
    AS SELECT DISTINCT * FROM fact_asignaciones
""")

# Verificar tabla contiene datos
spark.sql("SELECT COUNT(*) FROM operaciones.gold_kvp.fact_asignaciones").show()