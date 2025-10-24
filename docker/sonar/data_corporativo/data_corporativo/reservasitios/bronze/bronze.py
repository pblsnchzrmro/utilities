# Databricks notebook source
# MAGIC %pip install sshtunnel
# MAGIC
# MAGIC import pandas as pd
# MAGIC from sshtunnel import SSHTunnelForwarder,logging
# MAGIC import psycopg2 as pg
# MAGIC from pyspark.sql import functions as F
# MAGIC from datetime import datetime, timedelta
# MAGIC from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "Workspace/data_corporativo/unhiberse/utils/utils"

# COMMAND ----------

Postgre_ssh_host = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-ssh-host")
Postgre_ssh_username = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-ssh-username")
Postgre_ssh_password = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-ssh-password")
Postgre_DB_username = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-DB-username")
Postgre_DB_password = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-DB-password")
Postgre_DB_name = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-DB-name")
localhost = '127.0.0.1'

# COMMAND ----------

ssh_tunnel = SSHTunnelForwarder(
    (Postgre_ssh_host, 22),
    ssh_username = Postgre_ssh_username,
    ssh_password = Postgre_ssh_password,
    remote_bind_address = (localhost, 5432)
 )
ssh_tunnel.start()
print("Started tunnel")

# COMMAND ----------

conn = pg.connect(
       host=localhost,
       port=ssh_tunnel.local_bind_port,
       user=Postgre_DB_username,
       password= Postgre_DB_password,
       database=Postgre_DB_name)

# COMMAND ----------

print('Executing SQL Query & Fetching Results...')
db_cursor = conn.cursor()
query = """SELECT *  FROM "DepartmentsUser_View";"""
df = pd.read_sql_query(query, conn)
df = spark.createDataFrame(df)
# insert_Azure_SQL(df,"[STG].[DepartmentsUser_View]",mode="append")
insert_Azure_SQL(df,"[RESERVA].[DepartmentsUser]",mode="append")