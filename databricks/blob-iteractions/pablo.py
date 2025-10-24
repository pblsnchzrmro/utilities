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

def get_client(file_system_name: str) -> FileSystemClient:
    """Get the file system client with the current connection string and the file system name."""
    connection_string = ""
    datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = datalake_service_client.get_file_system_client(file_system_name)
    return file_system_client

def get_container_client(container_name: str = "ficheros") -> ContainerClient:
    """Get the container client with the current connection string and the container name."""
    connection_string = ""
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client

def upload_file_from_bytes(file_contents: BytesIO, filename: str, path: str, file_system_name: str = "all-ficheros-text") -> None:
    """Upload the file contents stored in a BytesIO object."""
    if file_contents.getvalue():
        file_system_client = get_client(file_system_name)
        file = os.path.join(path, filename)
        file_client = file_system_client.create_file(file)
        file_contents.seek(0)
        file_client.append_data(data=file_contents.getvalue(), offset=0, length=len(file_contents.getvalue()))
        file_client.flush_data(len(file_contents.getvalue()))

def copy_files(src_container_name: str, dest_container_name: str, supported_extensions: list, src_folder: str, path: str = "/") -> None:
    src_container_client = get_container_client(src_container_name)
    blob_list = list(src_container_client.list_blobs())  # transform to list
    total_blobs = len(blob_list)  # get the total count
    for blob in tqdm(blob_list, desc='Copying files', total=total_blobs, unit='file'):
        blob_name = blob.name
        filename, file_extension = os.path.splitext(blob_name)
        new_filename = f"{os.path.basename(filename)}-{uuid.uuid4()}"
        if file_extension not in supported_extensions:
            new_filename += '.txt'
        else:
            new_filename += file_extension
        blob_data = src_container_client.download_blob(blob_name)
        with BytesIO() as blob_bytes:
            blob_data.download_to_stream(blob_bytes)
            upload_file_from_bytes(blob_bytes, new_filename, path, dest_container_name)



supported_extensions = [".txt", ".pdf", ".html", ".md", ".doc", ".docx", ".dot", ".dotx", ".ppt", ".pptx", ".pot", ".potx", ".pps", ".ppsx"]
copy_files("ficheros", "all_ficheros_text", supported_extensions, "/gitlab/112")
