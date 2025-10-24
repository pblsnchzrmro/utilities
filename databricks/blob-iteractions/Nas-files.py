# Databricks notebook source
!python -m pip install --upgrade pip -q

# COMMAND ----------

!pip install tqdm -q

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.filedatalake._file_system_client import FileSystemClient
from azure.storage.blob import BlobClient
import os

from io import BytesIO, StringIO
from tqdm import tqdm
import io
import uuid
import glob

# COMMAND ----------

def get_client_seguitur(file_system_name: str) -> FileSystemClient:
  """Get the file system client with the current connection string and the file system name.
  """
  connection_string = ""

  datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)

  file_system_client = datalake_service_client.get_file_system_client(file_system_name)
  
  return file_system_client

# COMMAND ----------

def get_container_client(container_name: str = "ficherosdfs") -> ContainerClient:
  """Get the container client with the current connection string and the container name
  """
  connection_string = ""
  
  blob_service_client = BlobServiceClient.from_connection_string(connection_string)

  container_client = blob_service_client.get_container_client(container_name)
  
  return container_client

# COMMAND ----------

storageAccountName = "seguiturdatalake"
storageAccountAccessKey = ''
blobContainerName = "NAS-Dgos-files"
dbutils.fs.mount(
source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
mount_point = f"/mnt/{blobContainerName}/",
extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})

# COMMAND ----------

import glob
import os

def get_all_files(directory):
  files = []
  dirs = os.walk(directory)
  total_dirs = len(list(dirs)) 
  dirs = os.walk(directory)
  for root, _, filenames in tqdm(dirs, total=total_dirs):
    for filename in filenames:
      files.append(os.path.join(root, filename))
  return files

# COMMAND ----------

blobs = get_all_files("/dbfs/mnt/ficherosdfs/DGOS/TERRITORIALES/Andrés Flores")

# COMMAND ----------

print(blobs)

# COMMAND ----------

destination_connection_string = ""
destination_container_name = "nas-dgos-files"


# COMMAND ----------

supported_extensions = [".txt", ".pdf", ".html", ".md", ".doc", ".docx", ".dot", ".dotx", ".ppt", ".pptx", ".pot", ".potx", ".pps", ".ppsx"]

def copy_files(destination_connection_string, destination_container_name):
    destination_blob_service_client = BlobServiceClient.from_connection_string(destination_connection_string)
    
    destination_container_client = destination_blob_service_client.get_container_client(destination_container_name)
    
    for blob in tqdm(blobs, total= len(blobs)):
      blob_name = blob.split("/")[-1]
      if any(blob_name.endswith(file_format) for file_format in supported_extensions):
          
        bloby = blob_name.rsplit(".", 1)
        # print(bloby)
        blo_name = bloby[0] + "-" + str(uuid.uuid4()) + "." + bloby[-1]

        destination_blob_client = destination_blob_service_client.get_blob_client(container=destination_container_name, blob= blo_name)
        with open(blob, "rb") as file1:
          destination_blob_client.upload_blob(file1)
          

copy_files( destination_connection_string, destination_container_name)