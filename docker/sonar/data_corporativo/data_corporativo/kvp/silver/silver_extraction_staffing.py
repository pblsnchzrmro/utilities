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

# spark.sql("CREATE SCHEMA IF NOT EXISTS operaciones.silver_kvp")

# COMMAND ----------

# Conexión KVP
kvp_url="https://kvpproject.hiberus.com/usuarios/auth"
 
payload = json.dumps({
  "username": "ZZZ_DATA",
  "password": "8b84cb84e3e151abe4fccb3dfd7f67d8",
  "azure": False
})
headers = {
  'Content-Type': 'application/json'
}

# Se inicializa el dataframe a vacío
schema = StructType([])
kvp_full_df = spark.createDataFrame([],schema)

response = requests.request("POST", kvp_url, headers=headers, data=payload)

if response.status_code == 200:
    formatted_res = response.json()
    token = formatted_res['token']
    
    # Headers
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "es,es-ES;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "origin": "https://kvpproject.hiberus.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://kvpproject.hiberus.com/staffing/people",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "Authorization": "Bearer " + token
    }

        
    final_url = "https://kvpproject.hiberus.com/api/staffing/filter-staffing"

    # Data (payload)
    data = {
        "empleado": [],
        "proyecto": [],
        "modo": True,
        "dedicacionDesde": 0,
        "dedicacionHasta": 10000,
        "dimensiones": [
            {"idObjeto": 1967, "idDimension": 3, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 4, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 5, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 6, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 7, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 8, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 9, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 10, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 11, "idDimensionValoresModificar": []},
            {"idObjeto": 1967, "idDimension": 12, "idDimensionValoresModificar": []}
        ],
        "puestoTrabajo": None
    }
            
    get_res = requests.request("POST", final_url, headers=headers, json=data)
        
    if get_res.status_code == 200:
        datos_json = get_res.json()
        if isinstance(datos_json, list):
            if datos_json:
                sample_data = datos_json[0]
                schema = StructType([StructField(key, StringType(), True) for key in sample_data.keys()])
                df = spark.createDataFrame(datos_json, schema)
            else:
                df = spark.createDataFrame([], schema)
        else:
            sample_data = datos_json
            schema = StructType([StructField(key, StringType(), True) for key in sample_data.keys()])
            df = spark.createDataFrame([datos_json], schema)

        if kvp_full_df.isEmpty():
            kvp_full_df = df
        else:
            kvp_full_df = kvp_full_df.union(df)


    else:
        print("Error occurred while making API request due to: ")
        print(get_res.content)
            

    get_res.close()
    time.sleep(1)

else:
    print("Error occurred while making API request due to: ")
    print(response.content)
    

# COMMAND ----------

# Vista
kvp_full_df.createOrReplaceTempView("kvp_full_df")

# Guardar como Delta table
spark.sql("""
    CREATE OR REPLACE TABLE operaciones.silver_kvp.staffing 
    AS SELECT DISTINCT * FROM kvp_full_df
""")

# Verificar tabla contiene datos
spark.sql("SELECT COUNT(*) FROM operaciones.silver_kvp.staffing").show()