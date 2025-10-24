# Databricks notebook source
# Install required libraries (use %pip for Databricks runtime 7.3+)
#%pip install --upgrade pyodbc requests pandas

# Import libraries
#import pyodbc
#import requests
#import pandas as pd
#import json
#from datetime import datetime

# Verify the installations by printing version info
#print(f"pyodbc version: {pyodbc.__version__}")
#print(f"requests version: {requests.__version__}")
#print(f"pandas version: {pd.__version__}")


# COMMAND ----------

# DBTITLE 1,API MTE TOKEN
client_id = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE-token-client-id")
client_secret = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE-token-client-secret")
scope = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE-token-client-scope")

def API_MTE_token():
    # Conseguir el token en la API
    URL = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE-token-url")
    
    myobj = {'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': scope}
    
    r = requests.post(url = URL,data = myobj)
    data = r.json()
    print(data)
    token = data['access_token']
    header = {'Authorization': 'Bearer '+ token}
    return header

# COMMAND ----------

client_id_MTE2 = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-client-id")
client_secret_MTE2 = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-client-secret")
scope_MTE2 = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-client-scope")

#Función para extraer el token de la API 'MTE-API-Data'
def API_MTE_token_2(): 
        # Conseguir el token en la API: MTE-API-Data
        URL = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-url")

        myobj = {'client_id': client_id_MTE2,
            'client_secret': client_secret_MTE2,
            'grant_type': 'client_credentials',
            'scope': scope_MTE2}

        r = requests.post(url = URL,data = myobj)
        data = r.json()
        token = data['access_token']
        header = {'Authorization': 'Bearer '+ token}
        return header

# COMMAND ----------

AzureDB_Hostname = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Azure-BD-Hostname")
AzureDB_Database = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Azure-BD-Database")
AzureDB_Datauser = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Azure-BD-Datauser")
AzureDB_Datapassword = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Azure-BD-Datapassword")

# COMMAND ----------

# Insertar dataframe en la base de datos Azure SQL
def insert_Azure_SQL(dataframe,sql_table,mode="append",overwrite='Y'):
    jdbcHostname = AzureDB_Hostname
    jdbcDatabase = AzureDB_Database
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : AzureDB_Datauser,
      "password" : AzureDB_Datapassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    if overwrite == 'Y':
      conn = db_conn()
      cursor = conn.cursor()
      rowsdeleted = cursor.execute("DELETE FROM {}".format(sql_table)).rowcount
      print("deleted: " + str(rowsdeleted))
      cursor.close()
      conn.commit()
      conn.close()
    
    print("Cargando datos en la tabla" + sql_table)
    dataframe.write.jdbc(url=jdbcUrl,table=sql_table,mode=mode,properties=connectionProperties)

# COMMAND ----------

from datetime import datetime


# Delete datos en una tabla en la base de datos Azure SQL
def delete_data_Azure_SQL_given_field(sql_table, fieldName, valueToRemove):
    jdbcHostname = AzureDB_Hostname
    jdbcDatabase = AzureDB_Database
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : AzureDB_Datauser,
      "password" : AzureDB_Datapassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    conn = db_conn()
    cursor = conn.cursor()
    print("Borrando los datos de la tabla {} ".format(sql_table) + f" dónde el campo {fieldName} sea igual a {valueToRemove}")
    rowsdeleted = cursor.execute(f"DELETE FROM {sql_table} WHERE {fieldName} = '{valueToRemove}'").rowcount
    print("deleted: " + str(rowsdeleted))
    conn.commit()

def delete_data_Azure_SQL_given_field_plus_range(sql_table, fieldName, startValue, endValue):
    jdbcHostname = AzureDB_Hostname
    jdbcDatabase = AzureDB_Database
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : AzureDB_Datauser,
      "password" : AzureDB_Datapassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    conn = db_conn()
    cursor = conn.cursor()
    print("Borrando los datos de la tabla {} ".format(sql_table) + f" dónde el campo {fieldName} sea encuentre entre {startValue} y {endValue}")
    rowsdeleted = cursor.execute(f"DELETE FROM {sql_table} WHERE (FORMAT({fieldName}, 'yyyy-MM-dd') >= '{startValue}') AND (FORMAT({fieldName}, 'yyyy-MM-dd') <= '{endValue}')").rowcount
    print("deleted: " + str(rowsdeleted))
    conn.commit()

def delete_data_Azure_SQL_AnyoMes(sql_table,anyo,mes):
    jdbcHostname = AzureDB_Hostname
    jdbcDatabase = AzureDB_Database
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : AzureDB_Datauser,
      "password" : AzureDB_Datapassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    conn = db_conn()
    cursor = conn.cursor()
    print("Borrando los datos de la tabla {} ".format(sql_table) + f" para el año {anyo} y mes {mes}")
    rowsdeleted = cursor.execute(f"DELETE FROM {sql_table} WHERE ANYO = {anyo} AND MES = {mes}").rowcount
    print("deleted: " + str(rowsdeleted))
    conn.commit()

def delete_data_Azure_SQL_Anyo(sql_table,anyo):
    jdbcHostname = AzureDB_Hostname
    jdbcDatabase = AzureDB_Database
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : AzureDB_Datauser,
      "password" : AzureDB_Datapassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    conn = db_conn()
    cursor = conn.cursor()
    print("Borrando los datos de la tabla {} ".format(sql_table) + f" para el año {anyo}")
    rowsdeleted = cursor.execute(f"DELETE FROM {sql_table} WHERE ANYO = {anyo}").rowcount
    print("deleted: " + str(rowsdeleted))
    conn.commit()

# COMMAND ----------

def extract_Azure_SQL(sql_table):
    jdbcHostname = AzureDB_Hostname
    jdbcDatabase = AzureDB_Database
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
      "user" : AzureDB_Datauser,
      "password" : AzureDB_Datapassword,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    df = spark.read.jdbc(url=jdbcUrl, table=sql_table, properties=connectionProperties)    
    
    return df

# df = extract_Azure_SQL('[MTE].[DIMENSION_TERRITORIOS]')
# df.display()

# COMMAND ----------

import pyodbc
# import pandas as pd

def db_conn():
    server = AzureDB_Hostname
    database = AzureDB_Database
    username = AzureDB_Datauser
    password = AzureDB_Datapassword
    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                            "Server=" + server + ";"
                            "Database=" + database + ";"
                            "uid=" + username + ";pwd=" + password)
    return conn

# conn = db_conn()

# COMMAND ----------

# Ejecutar un procedure
def execute_procedure(procedure):
    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute("EXEC {}".format(procedure))
    print("Ejecutando el procedimiento " + procedure)
    rows = cursor.fetchall()
    for row in rows:
        print("Número de registros " + row[0]+ ": " + str(row[1]))
    cursor.close()
    conn.commit()
    conn.close()

# COMMAND ----------

# Solo hay que lanzarlo para montar el blob la primera vez.
# storage_account_key = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Storage-Account-Key")
# storage_account_name = "mtcdatalake2"
# container = "datamtc"
# spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# COMMAND ----------

# dbutils.fs.mount(
#  source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
#  mount_point = "/mnt/"+container,
#  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
# )

# dbutils.fs.unmount('/mnt/'+container)

# COMMAND ----------

# dbutils.fs.ls ("/mnt/datamtc/data/mte_API")


# COMMAND ----------

# import pandas as pd
# query = f"""SELECT * FROM [MTE].[STG_DIMENSION_USUARIOS]"""
# df = pd.read_sql_query(query, conn)
# df.display()

# COMMAND ----------

# def get_external_ip(x):
#     import requests
#     import socket

#     hostname = socket.gethostname()
#     r = requests.get("https://api.ipify.org/")
#     public_IP = r.content
#     return(f"#{x} From {hostname} with publicIP {public_IP}.")

# print('DRIVER:')
# rdd1 = get_external_ip(0)
# print(rdd1)
# print('WORKERS:')   
# rdd2 = sc.parallelize(range(1, 4)).map(get_external_ip)
# datacoll2 = rdd2.collect()
# for row in datacoll2:
#     print(row)

# COMMAND ----------

# MAGIC %sh
# MAGIC # curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC # curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC
# MAGIC # apt-get update
# MAGIC # ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC # apt-get -y install unixodbc-dev
# MAGIC # sudo apt-get install python3-pip -y
# MAGIC # pip3 install --upgrade pyodbc