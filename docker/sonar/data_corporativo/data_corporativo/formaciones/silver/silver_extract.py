# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, ArrayType
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

spark = SparkSession.builder.appName("JSON Parsing").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculo de la fecha inicial y final del mes pasado

# COMMAND ----------

def first_last_month(month: int, year: int):
    first_day = datetime(year, month, 1)
    
    # El último día se obtiene sumando un mes al primer día y restando un día
    if month == 12:
        last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(days=1)
    
    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")


# COMMAND ----------

# full_load = 'Y' 
full_load = 'N'

# COMMAND ----------

if full_load == 'Y':
  first_day = '2021-01-01' # primera fecha es 2023-08-02
  prev_date = datetime.today() - relativedelta(months=1)
  month, year = prev_date.month, prev_date.year
  x, last_day = first_last_month(month, year)

if full_load == 'N':
  prev_date = datetime.today() - relativedelta(months=1)
  month, year = prev_date.month, prev_date.year
  first_day, last_day = first_last_month(month, year)

# COMMAND ----------

print(first_day, last_day)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Llamada a la API

# COMMAND ----------

url = "https://huniversity-api.hiberus.com/huniversity/precourse/precourse/casualty-prediction-report"
secret_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3Mjg0NjgwMTYsImV4cCI6NDg4MjA2ODAxNiwicm9sZXMiOltdLCJ0eXBlVXNlciI6ImFwaSIsInVzZXJuYW1lIjoidXNlcmFwaSIsImlkRW50aXR5IjoxfQ.TEkIVX_Tlin79DXD0rM3ylxfxN6aS550SZ4hTlegEMndPs44ZuphhtqNalzacknxVQ1Al67NATXcW69B2dtNTzN-LCI9ktC1ifhodBCJE_7rFqLMEnYlzDnZLbfd9TAupFhyv1ZSquG7Cl9Iavk9ksWJAVk9VMGB7eQQXxm5fcxB8ucpsOOrwz2eLX8obdhq87Znyql6y7ZyyK3H9msPqFnTIPseDMopTnkEcUO6iYFhA2a8Gn5qSkwuLA8CFqT_H1BK-4k8fpvSeTVNmRVBky-RAAogylWtBkzsKB41SPlLr949KzuY7nzhBu2i5YYabe5AnNluSmDA6o4kG-SRykWsWAsTBeA1mPCV7aR8e8wCafyIz3w3oN8_pIY20pC_EoeFSxTcmyhEUaUfYLYybESz09_FC7QCDFHEzv5gRslKHTygn6r_AgyYwP4eAHvBnGj7FmKCsbBBBzNfr0GlKy-E_yH0-0y-90pvRRBi1toc2UR5gnvfAG6ZpR1jf3ZmNcH-xFbKxYQG1pJg8eNPG18kE3b-H-F22SWdkO6DPXjHYKElAWH1ZbFgmWsrJpCikFrsAG59IJi2x34_V1YMtG08Yc0C8j43DUB8vNxrrqUvyMIINQBjMwkbB7_lxTlIPIOQA3TjbwO7Ev6x1b-CBpyQmPIgC_CH3h9qeOxo0z8"    # Llevar a secret manager
body = {
    "tokenString": secret_key,
    "endDateFrom": first_day,
    "endDateTo": last_day
}

# COMMAND ----------

# DBTITLE 1,check conexion
try:
    response = requests.post(url, json=body)

    # Comprobar el estado de la respuesta
    if response.status_code == 200:
        print("Respuesta exitosa:")
        data = response.json()
        # print(data)  # Imprimir respuesta en formato JSON
    else:
        print(f"Error: {response.status_code}")
        print(response.text)  # Imprimir el texto de la respuesta de error

except requests.exceptions.RequestException as e:
    print(f"Error en la solicitud: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revisión de los datos

# COMMAND ----------

# data['data'][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Armado del dataframe

# COMMAND ----------

schema_trainings = StructType([
    StructField("programa", StringType(), True),
    StructField("curso", StringType(), True),
    StructField("modalidad", StringType(), True),
    StructField("horas", StringType(), True),
    StructField("proveedor", StringType(), True),
    StructField("fechaInicio", StringType(), True),
    StructField("fechaFin", StringType(), True),
    StructField("alumnos", ArrayType(StructType([  # Lista de alumnos
        StructField("nombre", StringType(), True),
        StructField("email", StringType(), True),
        StructField("empresa", StringType(), True),
        StructField("hgz", StringType(), True),
        StructField("hgr", StringType(), True)
    ])), True)
])

# COMMAND ----------

df_trainings = spark.createDataFrame(data['data'], schema=schema_trainings)

# COMMAND ----------

df_exploded = df_trainings.withColumn("alumno", explode(df_trainings["alumnos"])).drop("alumnos")
df_trainings_final = df_exploded.select(
    "programa", "curso", "modalidad", "horas", "proveedor", "fechaInicio", "fechaFin",
    df_exploded.alumno["nombre"].alias("nombre"),
    df_exploded.alumno["email"].alias("email"),
    df_exploded.alumno["empresa"].alias("empresa"),
    df_exploded.alumno["hgz"].alias("hgz"),
    df_exploded.alumno["hgr"].alias("hgr")
)
df_trainings_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exportar el dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### Guardado de los datos del mes pasado

# COMMAND ----------

if full_load == 'Y':
  df_trainings_final.write.mode("overwrite").format("delta").saveAsTable("people.silver_formaciones.formaciones_api")

if full_load == 'N':
  key_columns = ["nombre", "curso", "fechaInicio", "fechaFin"]
  df_trainings_final.createOrReplaceTempView("new_trainings")
  spark.sql(f"""
      MERGE INTO people.silver_formaciones.formaciones_api target
      USING (SELECT * FROM new_trainings WHERE fechaInicio >= '{first_day}' OR fechaFin <= '{last_day}') 
      source ON {' AND '.join([f'target.{col} = source.{col}' for col in key_columns])}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
  """)