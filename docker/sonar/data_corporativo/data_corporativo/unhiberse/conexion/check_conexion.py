# Databricks notebook source
# DBTITLE 1,Utils
# MAGIC %run "/Workspace/data_corporativo/unhiberse/utils/utils"

# COMMAND ----------

# DBTITLE 1,Check IP
# MAGIC %sh
# MAGIC curl ifconfig.me

# COMMAND ----------

import pandas as pd

import subprocess; subprocess.check_call(['pip', 'install', 'mysql-connector-python'])

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

# DBTITLE 1,Entorno
entorno = "PRE"

# COMMAND ----------

# DBTITLE 1,Keys
Unhiberse_host = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-host")
Unhiberse_user = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-user")
Unhiberse_pass = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-pass")
Unhiberse_DB = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Unhiberse-DB")

# COMMAND ----------

if entorno=="PRE":
    #Datos de conexón
    pgSQL_driver = "org.postgresql.Driver"

    pgSQL_host = Unhiberse_host
    pgSQL_port = "5432" # update if you use a non-default port
    #server = f"{database_host}:{database_port}"
    pgSQL_dbname = Unhiberse_DB 
    pgSQL_user = Unhiberse_user
    pgSQL_password = Unhiberse_pass

    pgSQL_url = f"jdbc:postgresql://{pgSQL_host}:{pgSQL_port}/{pgSQL_dbname}"

    print(pgSQL_url)

# COMMAND ----------

# DBTITLE 1,Conexion
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

# DBTITLE 1,Check Driver ODBC 17
# MAGIC %sh
# MAGIC odbcinst -q -d