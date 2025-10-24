# Databricks notebook source
# MAGIC %pip install office365-rest-python-client
# MAGIC
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC from office365.sharepoint.client_context import ClientContext
# MAGIC from office365.runtime.auth.authentication_context import AuthenticationContext
# MAGIC from office365.sharepoint.files.file import File
# MAGIC from pyspark.sql.functions import lit,date_format,current_timestamp
# MAGIC from delta.tables import DeltaTable
# MAGIC import re
# MAGIC
# MAGIC import pandas as pd

# COMMAND ----------

# Secretos
USERNAME = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Username")
PASSWORD = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Password")

# COMMAND ----------

# SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/hiberusData"
# SHAREPOINT_SITE_NAME = "hiberusData"
# SHAREPOINT_DOC = "Shared Documents/"

# SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/PowerBI"
# SHAREPOINT_SITE_NAME = "PowerBI"
# SHAREPOINT_DOC = "Shared Documents/"

# COMMAND ----------

container = "datamtcext" #broze container
connection_string = ""

# COMMAND ----------

def upload_to_blob(azure_path,file,fileName):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container="datamtc")
    if not container_client.exists():
        print("container does not exist. create it")
    
    if container_client.exists():
        blob = BlobClient.from_connection_string(conn_str=connection_string,
                                                        container_name="datamtc",
                                                        blob_name=fileName)
        azure_path = azure_path + fileName
        blob_client = container_client.get_blob_client(azure_path)
        blob_client.upload_blob(file,overwrite=True)

# COMMAND ----------

def compara_ficheros(fileName,TimeLastModified):    
    df = spark.sql(f"SELECT Nombre_Fichero,Fecha_Modificacion_Fichero FROM people.bronze_cobee.descarga_ficheros WHERE Nombre_Fichero = '{fileName}'")    
    
    if len(df.head(1)) == 0:
        print("El fichero es nuevo")
        descargar = 1
    else:
        Fecha = df.select("Fecha_Modificacion_Fichero")
        Fecha = Fecha.collect()[0][0]
        if Fecha == TimeLastModified:
            print("El fichero no ha sido modificado, por lo que no se descargará")
            descargar = 0
        else:
            print("El fichero "+ fileName + " se ha modificado y se descargará la nueva versión")
            descargar = 1
            
    return descargar

def actualizar_descarga_ficheros(fileName,TimeLastModified,FOLDER_DEST):
    df = pd.DataFrame(columns=['Nombre_Fichero', 'Fecha_Modificacion_Fichero', 'FECHA_ACT'])
    df.loc[0,'Nombre_Fichero'] = fileName
    df.loc[0,'Fecha_Modificacion_Fichero'] = TimeLastModified
    df.loc[0,'Carpeta'] = FOLDER_DEST
    
    df = spark.createDataFrame(df)
    df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

    print('Realizando el insert/update en la tabla de Descarga_Ficheros')

    delta_df = DeltaTable.forPath(spark, "dbfs:/mnt/datamtc/data/1-extract/cma_shp/ingesta_cma.Descarga_Ficheros")
    
    delta_df.alias("target").merge(
        source = df.alias("source"),
        condition = "target.Nombre_Fichero = source.Nombre_Fichero"
        ).whenMatchedUpdate(set =
                            {"Fecha_Modificacion_Fichero":"source.Fecha_Modificacion_Fichero",
                             "Carpeta":"source.Carpeta",
                             "FECHA_ACT":"source.FECHA_ACT"
                            }
        ).whenNotMatchedInsert(values =
                               {"Nombre_Fichero":"source.Nombre_Fichero",
                                "Fecha_Modificacion_Fichero":"source.Fecha_Modificacion_Fichero",
                                "Carpeta":"source.Carpeta",
                                "FECHA_ACT":"source.FECHA_ACT"}
        ).execute()

# COMMAND ----------

def conn_shp():
    ctx_auth = AuthenticationContext(SHAREPOINT_SITE)
    ctx_auth.acquire_token_for_user(USERNAME, PASSWORD)   
    conn = ClientContext(SHAREPOINT_SITE, ctx_auth)
    return conn

def get_files_list(folder_name):
    conn = conn_shp()
    target_folder_url = f'{SHAREPOINT_DOC}/{folder_name}'
    root_folder = conn.web.get_folder_by_server_relative_url(target_folder_url)
    root_folder.expand(["Files","Folders"]).get().execute_query()
    
    return root_folder.files

def download_file(file_name,folder_name):
    conn = conn_shp()
    file_url = f'/sites/{SHAREPOINT_SITE_NAME}/{SHAREPOINT_DOC}/{folder_name}/{file_name}'
    file = File.open_binary(conn, file_url)
    
    return file.content
    
def download_files(folder_name):
    return get_files_list(folder_name)

# COMMAND ----------

def save_file(file_n, file_obj, folder_dest,TimeLastModified):
    print("Guardando el archivo en " + folder_dest + file_n)    
    upload_to_blob(folder_dest,file_obj,file_n)
    actualizar_descarga_ficheros(file_n,TimeLastModified,folder_dest)

def get_file(file_n, folder, folder_dest,TimeLastModified):
    file_obj = download_file(file_n,folder)
    save_file(file_n, file_obj,folder_dest,TimeLastModified)
    
def get_files(folder,folder_dest):
    files_list = download_files(folder)
    for file in files_list:
        FileName = file.name
        TimeLastModified = file.properties['TimeLastModified']
        d = compara_ficheros(FileName,TimeLastModified)
        if d == 1:
            get_file(file.name,folder,folder_dest,TimeLastModified)

def get_files_by_pattern(keyword,folder,folder_dest):
    files_list = get_files_list(folder)
    for file in files_list:
        if re.search(keyword, file.name):
            FileName = file.name
            TimeLastModified = file.properties['TimeLastModified']
            d = compara_ficheros(FileName,TimeLastModified)
            if d == 1:
                get_file(file.name,folder,folder_dest,TimeLastModified)

# COMMAND ----------

def download_files_shp(FOLDER_NAME,FOLDER_DEST):
    get_files(FOLDER_NAME,FOLDER_DEST)

# COMMAND ----------

def download_files_shp_pattern(KEYWORD,FOLDER_NAME,FOLDER_DEST):
    get_files_by_pattern(KEYWORD,FOLDER_NAME,FOLDER_DEST)

# COMMAND ----------

def upload_file(file_name, folder_name, content):
    conn = conn_shp()
    target_folder_url = f'/sites/{SHAREPOINT_SITE_NAME}/{SHAREPOINT_DOC}/{folder_name}'
    target_folder_url = conn.web.get_folder_by_server_relative_path(target_folder_url)
    response = target_folder_url.upload_file(file_name, content).execute_query()
    
    return response

def get_file_content(file_path):
    with open(file_path, 'rb') as f:
        return f.read()

def get_list_of_files(folder):
    file_list = []
    folder_item_list = os.listdir(folder)
    for item in folder_item_list:
        item_full_path = folder +'/' + item
        if os.path.isfile(item_full_path):
            file_list.append([item, item_full_path])
    
    return file_list
