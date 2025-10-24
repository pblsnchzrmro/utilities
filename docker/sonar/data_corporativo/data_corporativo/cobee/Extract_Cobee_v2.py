# Databricks notebook source
import re
import datetime
import calendar
import os
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS people.bronze_cobee

# COMMAND ----------

# MAGIC %run "/Workspace/data_corporativo/common/utils_sharepoint"

# COMMAND ----------

catalog_name = "people"
schema_name = "bronze_cobee"

# COMMAND ----------

SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/Talento946"
SHAREPOINT_SITE_NAME = "Talento946"
SHAREPOINT_DOC = "Documentos compartidos"

# COMMAND ----------

FOLDER_NAME = 'General/FicherosVictor/Cobee'
FOLDER_DEST = f"data/0-staging/ingesta_ia/Ficheros_RRHH/"

# COMMAND ----------

ctx_auth = AuthenticationContext(SHAREPOINT_SITE)
ctx_auth.acquire_token_for_user(USERNAME, PASSWORD)
ctx = ClientContext(SHAREPOINT_SITE, ctx_auth)
web = ctx.web
ctx.load(web)
ctx.execute_query()

for KEYWORD in ['comida 2022', 'comida 2023','comida 2024','formacion 2022','formacion 2023','formacion 2024', 'guarderia 2022', 'guarderia 2023', 'guarderia 2024', 'seguro medico 2022', 'seguro medico 2023', 'seguro medico 2024', 'transporte 2022', 'transporte 2023', 'transporte 2024']:
    file_url = f"{SHAREPOINT_SITE}/{SHAREPOINT_DOC}/{KEYWORD}.xlsx"
    response = File.open_binary(ctx, file_url)
    
    with open("/tmp/temp_file.xlsx", "wb") as local_file:
        local_file.write(response.content)

    # Read the file into a DataFrame
    df = pd.read_excel("/tmp/temp_file.xlsx")
    table_name = KEYWORD  # Use the cleaned keyword as table name
    spark_df = spark.createDataFrame(df)

    # Save DataFrame to the catalog
    spark_df.write.format("delta").mode("overwrite").saveAsTable(f"people.bronze_cobee.{table_name}")

# COMMAND ----------

for KEYWORD in ['comida 2022', 'comida 2023','comida 2024','formacion 2022','formacion 2023','formacion 2024', 'guarderia 2022', 'guarderia 2023', 'guarderia 2024', 'seguro medico 2022', 'seguro medico 2023', 'seguro medico 2024', 'transporte 2022', 'transporte 2023', 'transporte 2024']:
    download_files_shp_pattern(KEYWORD,FOLDER_NAME,FOLDER_DEST)

# COMMAND ----------

def clean_column_name(name):
    # Reemplazar espacios por guiones bajos
    name = re.sub(r'\s+', '_', name)
    # Reemplazar la ñ por n
    name = name.replace('ñ', 'n').replace('Ñ', 'N')
    # Eliminar caracteres que no sean alfanuméricos o guiones bajos
    name = re.sub(r'[^\w_]', '', name)
    # Convertir a minúsculas
    name = name.lower()
    return name

# COMMAND ----------

def group_by_employee(result, year):
    result.drop('Empresa')
    if year == '2024':
        result_filtered = result.groupBy("Nombre","EMAIL","DNI").agg(
            F.max("Enero").alias("Enero"),
            F.max("Febrero").alias("Febrero"),
            F.max("Marzo").alias("Marzo"),
            F.max("Abril").alias("Abril"),
            F.max("Mayo").alias("Mayo"),
            F.max("Junio").alias("Junio"),
            F.max("Julio").alias("Julio"),
            F.max("Agosto").alias("Agosto"),
            F.max("Septiembre").alias("Septiembre"),
            F.max("Octubre").alias("Octubre"))
    else:
        result_filtered = result.groupBy("Nombre","EMAIL","DNI").agg(
            F.max("Enero").alias("Enero"),
            F.max("Febrero").alias("Febrero"),
            F.max("Marzo").alias("Marzo"),
            F.max("Abril").alias("Abril"),
            F.max("Mayo").alias("Mayo"),
            F.max("Junio").alias("Junio"),
            F.max("Julio").alias("Julio"),
            F.max("Agosto").alias("Agosto"),
            F.max("Septiembre").alias("Septiembre"),
            F.max("Octubre").alias("Octubre"),
            F.max("Noviembre").alias("Noviembre"),
            F.max("Diciembre").alias("Diciembre"))
    return result_filtered

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la tabla de Comida

# COMMAND ----------

for year in ['2022','2023','2024']:
    path = f"/dbfs/mnt/datamtc/data/0-staging/ingesta_ia/Ficheros_RRHH/comida {year}.xls"
    if os.path.exists(path):  # Verifica si el archivo existe
        print(f"Procesando archivo: {path}")
        # Lógica para procesar el archivo

        dfs = pd.read_excel(path,dtype=str)

        result = spark.createDataFrame(dfs)

        result = group_by_employee(result, year)

        cleaned_columns = [clean_column_name(col) for col in result.columns]
        result = result.toDF(*cleaned_columns)

        result.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.comida_{year}")
        spark.sql(f"""
                create table if not exists ingesta_ia.comida_{year}
                using delta
                location "dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.comida_{year}"
                """)
    else:
        print(f"Archivo no encontrado: {path}. Continuando con el siguiente año.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la tabla de Formación

# COMMAND ----------

for year in ['2022','2023','2024']:
    path = f"/dbfs/mnt/datamtc/data/0-staging/ingesta_ia/Ficheros_RRHH/formacion {year}.xls"
    if os.path.exists(path):  # Verifica si el archivo existe
        print(f"Procesando archivo: {path}")
        # Lógica para procesar el archivo

        dfs = pd.read_excel(path,dtype=str)

        result = spark.createDataFrame(dfs)

        result = group_by_employee(result, year)

        cleaned_columns = [clean_column_name(col) for col in result.columns]
        result = result.toDF(*cleaned_columns)

        result.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.formacion_{year}")
        spark.sql(f"""
                create table if not exists ingesta_ia.formacion_{year}
                using delta
                location "dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.formacion_{year}"
                """)
    else:
        print(f"Archivo no encontrado: {path}. Continuando con el siguiente año.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la tabla de Guarderia

# COMMAND ----------

for year in ['2022','2023','2024']:
    path = f"/dbfs/mnt/datamtc/data/0-staging/ingesta_ia/Ficheros_RRHH/guarderia {year}.xls"
    if os.path.exists(path):  # Verifica si el archivo existe
        print(f"Procesando archivo: {path}")
        # Lógica para procesar el archivo

        dfs = pd.read_excel(path,dtype=str)

        result = spark.createDataFrame(dfs)

        result = group_by_employee(result, year)

        cleaned_columns = [clean_column_name(col) for col in result.columns]
        result = result.toDF(*cleaned_columns)

        result.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.guarderia_{year}")
        spark.sql(f"""
                create table if not exists ingesta_ia.guarderia_{year}
                using delta
                location "dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.guarderia_{year}"
                """)
    else:
        print(f"Archivo no encontrado: {path}. Continuando con el siguiente año.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la tabla de Seguro Medico

# COMMAND ----------

for year in ['2022','2023','2024']:
    path = f"/dbfs/mnt/datamtc/data/0-staging/ingesta_ia/Ficheros_RRHH/seguro medico {year}.xls"
    if os.path.exists(path):  # Verifica si el archivo existe
        print(f"Procesando archivo: {path}")
        # Lógica para procesar el archivo

        dfs = pd.read_excel(path,dtype=str)

        result = spark.createDataFrame(dfs)

        result = group_by_employee(result, year)

        cleaned_columns = [clean_column_name(col) for col in result.columns]
        result = result.toDF(*cleaned_columns)

        result.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.seguro_medico_{year}")
        spark.sql(f"""
                create table if not exists ingesta_ia.seguro_medico_{year}
                using delta
                location "dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.seguro_medico_{year}"
                """)
    else:
        print(f"Archivo no encontrado: {path}. Continuando con el siguiente año.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la tabla de Transporte

# COMMAND ----------

for year in ['2022','2023','2024']:
    path = f"/dbfs/mnt/datamtc/data/0-staging/ingesta_ia/Ficheros_RRHH/transporte {year}.xls"
    if os.path.exists(path):  # Verifica si el archivo existe
        print(f"Procesando archivo: {path}")
        # Lógica para procesar el archivo

        dfs = pd.read_excel(path,dtype=str)

        result = spark.createDataFrame(dfs)

        result = group_by_employee(result, year)

        cleaned_columns = [clean_column_name(col) for col in result.columns]
        result = result.toDF(*cleaned_columns)

        result.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.transporte_{year}")
        spark.sql(f"""
                create table if not exists ingesta_ia.transporte_{year}
                using delta
                location "dbfs:/mnt/datamtc/data/IA_Talent/ingesta_ia.transporte_{year}"
                """)
    else:
        print(f"Archivo no encontrado: {path}. Continuando con el siguiente año.")

# COMMAND ----------

# Para chequeo de duplicados:
# spark.sql('SELECT nombre, email, dni, COUNT(*) AS registros FROM ingesta_ia.transporte_2024 GROUP BY nombre, email, dni HAVING registros > 1').display()