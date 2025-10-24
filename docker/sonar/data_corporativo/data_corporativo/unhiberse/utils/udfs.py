# Databricks notebook source
# DBTITLE 1,Imports
import pandas as pd
%pip install mysql-connector-python
import mysql.connector

import json
import requests
import datetime as dt
import glob
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import split
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import uuid
from pyspark.sql.functions import expr
from pyspark.sql.functions import when
import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import *
import numpy as np
from pyspark.sql.functions import current_date
from pyspark.sql.types import *

!pip install psycopg2
import psycopg2

# COMMAND ----------

# DBTITLE 1,Estableciendo Entorno
try:
    print(f"Entorno definido: {entorno}")
except NameError:
    print("Entorno definido por defecto: DEV")
    print("Ejecución en DEV")
    entorno="DEV"
else:
    if entorno.lower() == "dev".lower():
        print("Ejecución en DEV")
        entorno="DEV"
    elif entorno.lower() == "pre_pro".lower():
        print("Ejecución en PRE-PRO")
        entorno="PRE_PRO"
    elif entorno.lower() == "pre".lower():
        print("Ejecución en PRE")
        entorno="PRE"
    #elif entorno.lower() == "pro".lower():
    #    print("Ejecución en PRO")
    #    entorno="PRO"
    else:
        print("Entorno desconocido, definido por defecto: DEV")
        print("Ejecución en DEV")
        entorno="DEV"

# COMMAND ----------

# MAGIC %md
# MAGIC PostgreSQL
# MAGIC

# COMMAND ----------

# DBTITLE 1,Conexión PostgreSQL DEV
if entorno=="DEV":
    #Datos de conexón
    pgSQL_driver = "org.postgresql.Driver"

    pgSQL_host ="unhiberse2-dev.demohiberus.com" #"20.0.98.84"
    pgSQL_port = "5432" # update if you use a non-default port
    #server = f"{database_host}:{database_port}"
    pgSQL_dbname = "test-migration-unhiberse" # eg. postgres
    pgSQL_user = "sgraciasubira"
    pgSQL_password = "X*cM9GKLeS^7"

    pgSQL_url = f"jdbc:postgresql://{pgSQL_host}:{pgSQL_port}/{pgSQL_dbname}"

    print(pgSQL_url)

# COMMAND ----------

# DBTITLE 1,Conexión PostgreSQL PRE
if entorno=="PRE":
    #Datos de conexón
    pgSQL_driver = "org.postgresql.Driver"

    pgSQL_host = "unhiberse2-pre.demohiberus.com"
    pgSQL_port = "5432" # update if you use a non-default port
    #server = f"{database_host}:{database_port}"
    pgSQL_dbname = "unhibersedaily" # eg. postgres
    pgSQL_user = "unhiberseuser"
    pgSQL_password = "Developer00"

    pgSQL_url = f"jdbc:postgresql://{pgSQL_host}:{pgSQL_port}/{pgSQL_dbname}"

    print(pgSQL_url)

# COMMAND ----------

# DBTITLE 1,Conexión PostgreSQL PRE-PRO
if entorno=="PRE_PRO":
    #Datos de conexón
    pgSQL_driver = "org.postgresql.Driver"

    pgSQL_host ="unhiberse2-dev.demohiberus.com" #"20.0.98.84"
    pgSQL_port = "5432" # update if you use a non-default port
    #server = f"{database_host}:{database_port}"
    pgSQL_dbname = "test-migration-temp" # eg. postgres
    pgSQL_user = "sgraciasubira"
    pgSQL_password = "X*cM9GKLeS^7"

    pgSQL_url = f"jdbc:postgresql://{pgSQL_host}:{pgSQL_port}/{pgSQL_dbname}"

    print(pgSQL_url)

# COMMAND ----------

# DBTITLE 1,Conexión PostgreSQL PRO
#if entorno=="PRO":
#    #Datos de conexón
#    pgSQL_driver = "org.postgresql.Driver"
#
#    pgSQL_host = "pro-unhiberse-postgresql.postgres.database.azure.com"
#    pgSQL_port = "5432" # update if you use a non-default port
#    #server = f"{database_host}:{database_port}"
#    pgSQL_dbname = "unhiberse" # eg. postgres
#    pgSQL_user = "unhiberseuser"
#    pgSQL_password = "de@6gNGprURV"
#
#    pgSQL_url = f"jdbc:postgresql://{pgSQL_host}:{pgSQL_port}/{pgSQL_dbname}"
#
#    print(pgSQL_url)

# COMMAND ----------

# DBTITLE 1,Eliminando varible entorno
#import gc

#del entorno
#gc.collect()

# COMMAND ----------

# DBTITLE 1,read_pgSQL_data
def read_pgSQL_data(pgSQL_txTabla):
    remote_table = (spark.read
        .format("jdbc")
        .option("driver", pgSQL_driver)
        .option("url", pgSQL_url)
        .option("dbtable", pgSQL_txTabla)
        .option("user", pgSQL_user)
        .option("password", pgSQL_password)
        .load()
    )

    return remote_table

# COMMAND ----------

# DBTITLE 1,read_pgSQL_data_cleaned
def read_pgSQL_data_cleaned(pgSQL_txTabla):
    remote_table = (spark.read
        .format("jdbc")
        .option("driver", pgSQL_driver)
        .option("url", pgSQL_url)
        .option("dbtable", pgSQL_txTabla)
        .option("user", pgSQL_user)
        .option("password", pgSQL_password)
        .option("encoding", "UTF-8")
        .load()
    )

    return remote_table

# COMMAND ----------

# DBTITLE 1,truncate_pgSQL_table
def truncate_pgSQL_table(pgSQL_txTabla):
    # Crear una conexión a Postgres
    conn_pgSQL = psycopg2.connect(
    host=pgSQL_host,
    port= pgSQL_port,
    database=pgSQL_dbname,
    user=pgSQL_user,
    password=pgSQL_password    
    )
    # Crear un cursor para ejecutar comandos SQL
    cursor_pgSQL = conn_pgSQL.cursor()
    #sql_command = f"TRUNCATE {pgSQL_txTabla} RESTART IDENTITY CASCADE;"
    sql_command = f"TRUNCATE {pgSQL_txTabla} CASCADE;"

    cursor_pgSQL.execute(sql_command)

    conn_pgSQL.commit()
    cursor_pgSQL.close()
    conn_pgSQL.close()

# COMMAND ----------

# DBTITLE 1,write_pgSQL_data
def write_pgSQL_data(dfData,pgSQL_txTabla,truncate=1):
    if truncate == 1:
        truncate_pgSQL_table(pgSQL_txTabla)

    dfData.write \
    .format("jdbc") \
    .option("driver", pgSQL_driver) \
    .option("url", pgSQL_url) \
    .option("dbtable", pgSQL_txTabla) \
    .option("user", pgSQL_user) \
    .option("password", pgSQL_password) \
    .option("stringtype", "unspecified") \
    .mode("append") \
    .save()

# COMMAND ----------

# DBTITLE 1,write_pgSQL_data_UUID
def write_pgSQL_data_UUID(dfData,pgSQL_txQuery,pgSQL_txTabla,pgSQL_txPreQuery="",pgSQL_txPostQuery="",truncate=1):
    if truncate == 1:
        truncate_pgSQL_table(pgSQL_txTabla)

    # Crear una conexión a Postgres
    conn_pgSQL = psycopg2.connect(
    host=pgSQL_host,
    port= pgSQL_port,
    database=pgSQL_dbname,
    user=pgSQL_user,
    password=pgSQL_password    
    )

    # Crear un cursor para ejecutar comandos SQL
    cursor_pgSQL = conn_pgSQL.cursor()

    #DESACTIVO FK TEMPORALMENTE HASTA EL COMMIT
    if pgSQL_txPreQuery != "":
        cursor_pgSQL.execute(pgSQL_txPreQuery)

    #  EXECUTE PARAMETERIZED QUERY
    cursor_pgSQL.executemany(f"{pgSQL_txQuery}", dfData.toPandas().to_numpy().tolist())

    if pgSQL_txPostQuery != "":
        cursor_pgSQL.execute(pgSQL_txPostQuery)

    conn_pgSQL.commit()

    cursor_pgSQL.close()
    conn_pgSQL.close()

# COMMAND ----------

# DBTITLE 1,write_pgSQL_data_UUID_v2
def write_pgSQL_data_UUID_v2(dfData,pgSQL_txQuery,pgSQL_txTabla,pgSQL_txPreQuery="",pgSQL_txPostQuery="",truncate=1):
    if truncate == 1:
        truncate_pgSQL_table(pgSQL_txTabla)

    # Crear una conexión a Postgres
    conn_pgSQL = psycopg2.connect(
    host=pgSQL_host,
    port= pgSQL_port,
    database=pgSQL_dbname,
    user=pgSQL_user,
    password=pgSQL_password    
    )
    # Crear un cursor para ejecutar comandos SQL
    cursor_pgSQL = conn_pgSQL.cursor()

    #EJECUTO PRE_QUERY SI TIENE ALGO
    if pgSQL_txPreQuery != "":
        cursor_pgSQL.execute(pgSQL_txPreQuery)
        conn_pgSQL.commit()

    #EXECUTE PARAMETERIZED QUERY
    # Convertir el DataFrame a una lista de listas
    data_list = dfData.toPandas().to_numpy().tolist()

    # Dividir los datos en lotes de 10
    batch_size = 10
    batches = [data_list[i:i + batch_size] for i in range(0, len(data_list), batch_size)]

    # Ejecutar la inserción en lotes
    for batch in batches:
        for row in batch:
            try:
                cursor_pgSQL.execute(pgSQL_txQuery, row)
                conn_pgSQL.commit()  # Confirmar el lote actual
            except Exception as e:
                print(f"Error al insertar la fila: {row}")
                print(f"Excepción: {e}")
                conn_pgSQL.rollback()  # Revertir cambios en caso de error

    #EJECUTO POST_QUERY SI TIENE ALGO
    if pgSQL_txPostQuery != "":
        cursor_pgSQL.execute(pgSQL_txPostQuery)
        conn_pgSQL.commit()

    cursor_pgSQL.close()
    conn_pgSQL.close()

# COMMAND ----------

# DBTITLE 1,exec_query_pgSQL
def exec_query_pgSQL(pgSQL_txQuery):
    # Crear una conexión a Postgres
    conn_pgSQL = psycopg2.connect(
    host=pgSQL_host,
    port= pgSQL_port,
    database=pgSQL_dbname,
    user=pgSQL_user,
    password=pgSQL_password    
    )

    # Crear un cursor para ejecutar comandos SQL
    cursor_pgSQL = conn_pgSQL.cursor()
    cursor_pgSQL.execute(pgSQL_txQuery)

    #print(f"Ejecutado '{pgSQL_txQuery}'")

    conn_pgSQL.commit()
    cursor_pgSQL.close()
    conn_pgSQL.close()

# COMMAND ----------

# DBTITLE 1,exec_query_file_pgSQL
def exec_query_file_pgSQL(txQueryFilePath):
    print("")
    print(f"Ejecutando {txQueryFilePath}")
    with open(txQueryFilePath) as fr:
        query = fr.read()
    #print(query)
    exec_query_pgSQL(query)
    print(f"Ejecutado {txQueryFilePath}")

# COMMAND ----------

# DBTITLE 1,Prueba de Conexión pgSQL
# Establecer la conexión
pgSQL_conexion = psycopg2.connect(
    host=pgSQL_host,
    port= pgSQL_port,
    database=pgSQL_dbname,
    user=pgSQL_user,
    password=pgSQL_password    
    )
print("pgSQL_conexion Ok")

# COMMAND ----------

# MAGIC %md
# MAGIC MySQL

# COMMAND ----------

# DBTITLE 1,Conexión MySQL
Unhiberse_host = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-host")
Unhiberse_user = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-user")
Unhiberse_pass = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-pass")
Unhiberse_DB = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-DB")

MySQL_driver = "org.mariadb.jdbc.Driver"

MySQL_host = Unhiberse_host
MySQL_port = "3306" # update if you use a non-default port
MySQL_dbname = Unhiberse_DB
#table = "ter_grupos"
MySQL_user = Unhiberse_user
MySQL_password = Unhiberse_pass

MySQL_url = f"jdbc:mysql://{MySQL_host}:{MySQL_port}/{MySQL_dbname}"

#print(MySQL_url)

# COMMAND ----------

# DBTITLE 1,read_MySQL_data
def read_MySQL_data(MySQL_txTabla):
    remote_table = (spark.read
        .format("jdbc")
        .option("driver", MySQL_driver)
        .option("url", MySQL_url)
        .option("dbtable", MySQL_txTabla)
        .option("user", MySQL_user)
        .option("password", MySQL_password)
        .load()
    )

    return remote_table

# COMMAND ----------

# DBTITLE 1,read_MySQL_data_cleaned
def read_MySQL_data_cleaned(MySQL_txTabla):
    remote_table = (spark.read
        .format("jdbc")
        .option("driver", MySQL_driver)
        .option("url", MySQL_url)
        .option("dbtable", MySQL_txTabla)
        .option("user", MySQL_user)
        .option("password", MySQL_password)
        .option("encoding", "ISO-8859-1")
        .load()
    )

    return remote_table

# COMMAND ----------

# DBTITLE 1,Prueba de Conexión MySQL
# Establecer la conexión
MySQL_conexion = mysql.connector.connect(
    host = MySQL_host,
    database = MySQL_dbname,
    user = MySQL_user,
    password = MySQL_password    
)
print("MySQL_conexion Ok")