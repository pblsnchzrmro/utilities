# Databricks notebook source
!pip3 install openpyxl

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd
import datetime as dt
import glob
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
import re
import os
import random
import warnings
warnings.filterwarnings('ignore')
import calendar
import numpy as np
from datetime import datetime
import dateutil.relativedelta
import re
import datetime
from functools import reduce
from pyspark.sql.functions import trim
from pyspark.sql.functions import col
from pyspark.sql.connect.dataframe import DataFrame as dfs
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

# MAGIC %run "/Workspace/data_corporativo/analitico/utils/Utils_Sharepoint"

# COMMAND ----------

# MAGIC %run "/Workspace/data_corporativo/analitico/utils/exception"

# COMMAND ----------

# MAGIC %run "/Workspace/data_corporativo/analitico/utils/constants"

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.remoteFiltering.blockSelfJoins", "false")

# COMMAND ----------

# DBTITLE 1,Functions downloading
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

def grab_directory(folder_name):
    conn = conn_shp()
    target_folder_url = f'{SHAREPOINT_DOC}/{folder_name}'
    root_folder = conn.web.get_folder_by_server_relative_url(target_folder_url)
    root_folder.expand(["Files","Folders"]).get().execute_query()
    return root_folder

# This function returns the directories in the Sharepoint folder given a folder.
def obtain_company_list(folder_name):
    root_folder = grab_directory(folder_name)
    return root_folder.folders

# This function returns the files in the Sharepoint folder given a folder.
def obtain_file_list(folder_name):
    root_folder = grab_directory(folder_name)
    return root_folder.files

# Mainpoint. It downloads data from Sharepoint and saves it in the local folder.
def download_component(component):
    download, name_folder, name_folder_local, action, other = component
    if download == "Y":
        prefix_dir = f'{initial_prefix}/{name_folder}/{anyo_carga}/'
        if action:
            dim_empresas = list(obtain_company_list(prefix_dir))
            for empresa in dim_empresas:
                FOLDER_NAME = f'{prefix_dir}{empresa}/'
                FOLDER_DEST = f"{mounting_point}/{name_folder_local}/{anyo_carga}/{empresa}/"
                download_files_shp_pattern(KEYWORD, FOLDER_NAME, FOLDER_DEST)
        else:
            if other:
                FOLDER_NAME = f'{initial_prefix}/{name_folder}'
                files = list(map(lambda x: x.name , list(obtain_file_list(FOLDER_NAME))))
                FOLDER_DEST = f"{mounting_point}/{name_folder_local}/"
                for file in files:
                    download_files_shp_pattern(file, FOLDER_NAME, FOLDER_DEST)
            else:
                FOLDER_NAME = f'{prefix_dir}'
                FOLDER_DEST = f"{mounting_point}/{name_folder_local}/{anyo_carga}/"
                download_files_shp_pattern(KEYWORD, FOLDER_NAME, FOLDER_DEST)

# COMMAND ----------

# DBTITLE 1,Functions transformation
def get_date_format(year, month, day):
    return '{0}-{1}-{2}'.format(year, month, day)

def get_date_format_slash(year, month, day):
    return '{0}/{1}/{2}'.format(day, month, year)

def recursive_search_excel(directory, prefix):
    return list(map(lambda x: x[0], search_excel_with_company(directory, prefix)))

def search_excel_with_company(directory, prefix):
    files = []
    elements = dbutils.fs.ls(directory)
    for element in elements:
        if element.isFile() and element.path.endswith('.xlsx') and element.name.startswith(prefix):
            files.append((element.path, element.path.split('/')[-2]))
        elif element.isDir():
            files += search_excel_with_company(element.path, prefix)
    return files

def load_table_unity_catalog(spark, table_name, condition, result, part_year = None, part_month = None):
  if spark.catalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)
    deltaTable.delete(condition)
  if part_year and part_month:
    result.write.mode("append").format("delta").partitionBy(part_year, part_month).option("overwriteSchema", True).saveAsTable(table_name)
  else:
    result.write.mode("append").format("delta").saveAsTable(table_name)

def get_column_names(direction):
  xls = pd.ExcelFile(direction)
  sheet_names = xls.sheet_names
  xls.close()
  return sheet_names