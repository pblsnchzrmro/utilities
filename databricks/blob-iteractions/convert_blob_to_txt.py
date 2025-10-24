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

# def get_client_mayofest(file_system_name: str) -> FileSystemClient:
#   """Get the file system client with the current connection string and the file system name.
#   """
#   connection_string = ""

#   datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)

#   file_system_client = datalake_service_client.get_file_system_client(file_system_name)
  
#   return file_system_client

# COMMAND ----------

def get_client_seguitur(file_system_name: str) -> FileSystemClient:
  """Get the file system client with the current connection string and the file system name.
  """
  connection_string = ""

  datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)

  file_system_client = datalake_service_client.get_file_system_client(file_system_name)
  
  return file_system_client

# COMMAND ----------

def get_container_client(container_name: str = "ficheros") -> ContainerClient:
  """Get the container client with the current connection string and the container name
  """
  connection_string = ""
  
  blob_service_client = BlobServiceClient.from_connection_string(connection_string)

  container_client = blob_service_client.get_container_client(container_name)
  
  return container_client

# COMMAND ----------

storageAccountName = "mayofest2023"
storageAccountAccessKey = ''
blobContainerName = "ficherosdfs"
dbutils.fs.mount(
source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
mount_point = f"/mnt/{blobContainerName}/",
extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})

# COMMAND ----------

storageAccountName = "seguiturdatalake"
storageAccountAccessKey = ''
blobContainerName = "agrafo-files"
dbutils.fs.mount(
source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
mount_point = f"/mnt/{blobContainerName}/",
extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})

# COMMAND ----------

def upload_file_from_bytes(
  file_contents: BytesIO, 
  filename: str, 
  path: str, 
  file_system_name: str = "agrafo-files") -> None:
  """Upload the file contents stored in a BytesIO object.
  """
  file_system_client = get_client_seguitur(file_system_name)

  file = path + "/" + filename
  
  # print("Uploading file {} to {}...".format(file, file_system_name))

  file_client = file_system_client.create_file(file)

  file_contents.seek(0)

  file_client.append_data(data=file_contents, offset=0, length=len(file_contents.getvalue()))

  file_client.flush_data(len(file_contents.getvalue()))

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





# def get_all_files(directory):
#   files = []
#   for file_path in glob.glob(directory + '/*'):
#       if os.path.isfile(file_path):
#           files.append(file_path)
#       elif os.path.isdir(file_path):
#           files.extend(get_all_files(file_path))
#   return files

# path = "/"
# for file in get_all_files("/dbfs/mnt/blob-test-container/"):
#   filename = file.split("/")[-1]
#   with open(file, "rb") as file_1:
#     content = file_1.read()

#     bytes_io = io.BytesIO()
#     bytes_io.write(content)
    
#     filename_txt = filename.split(".")[0]
#     filename_extension = ".txt"
#     filename_upload = filename_txt + filename_extension
#     upload_file_from_bytes(bytes_io, filename_upload, path)


# COMMAND ----------

# blob_list  = get_all_files("/dbfs/mnt/ficheros/gitlab/112/")
folders = ["agrafo-back", "agrafo-front"]


blob_list = []
for folder in tqdm(folders, total=len(folders)):
  blob_list.extend(get_all_files(f"/dbfs/mnt/ficheros/gitlab/{folder}/"))

# COMMAND ----------

path = "/"
supported_extensions = [".txt", ".pdf", ".html", ".md", ".doc", ".docx", ".dot", ".dotx", ".ppt", ".pptx", ".pot", ".potx", ".pps", ".ppsx"]


for file_ in tqdm(blob_list, total=len(blob_list)):
    filename = file_.split("/")[-1]
    # print(filename)
    try:
      with open(file_, "rb") as file_1:
        content = file_1.read()

        bytes_io = io.BytesIO()
        bytes_io.write(content)

        if bytes_io.getvalue():
            filename_txt = filename.split(".")[0]
            filename_extension = ".txt" if not any(filename.endswith(ext) for ext in supported_extensions) else "." + filename.split(".")[1]
            filename_upload = filename_txt +"-" + str(uuid.uuid4()) + filename_extension
            # print(filename_upload)
            upload_file_from_bytes(bytes_io, filename_upload, path)

    except Exception as error:
      pass

# COMMAND ----------

len(blob_list)

# COMMAND ----------

with open(file_, "rb") as file_1:
  content = file_1.read()