# Databricks notebook source
# Install required packages and libraries
%pip install Office365-REST-Python-Client

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.request import SharePointRequest
import re
import datetime
import calendar
import pandas as pd
import seaborn as sns

# SharePoint connection details - REPLACE THESE VALUES
SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/CRMTD"
SHAREPOIT_LIST = "'Proyectos y oportunidades'"
USERNAME = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Username")
PASSWORD = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Password")

class SharePointRequest:
    def __init__(self, site_url):
        self.site_url = site_url
        self.credentials = None
        self.auth_cookie = None
        self.digest_value = None
        
    def with_credentials(self, credentials):
        self.credentials = credentials
        return self
        
    def execute_request(self, endpoint_url):
        # Get authentication cookie
        auth_url = f"{self.site_url}/_api/contextinfo"
        auth_response = requests.post(
            auth_url,
            auth=(self.credentials.username, self.credentials.password),
            headers={'Accept': 'application/json;odata=verbose'}
        )
        
        if auth_response.status_code != 200:
            raise Exception(f"Authentication failed with status code {auth_response.status_code}: {auth_response.text}")
            
        auth_data = auth_response.json()
        self.digest_value = auth_data['d']['GetContextWebInformation']['FormDigestValue']
        
        # Execute the actual request
        api_url = f"{self.site_url}/_api/{endpoint_url}"
        headers = {
            'Accept': 'application/json;odata=verbose',
            'Content-Type': 'application/json;odata=verbose',
            'X-RequestDigest': self.digest_value
        }
        
        response = requests.get(
            api_url,
            auth=(self.credentials.username, self.credentials.password),
            headers=headers
        )
        
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
            
        return response

def items_from_sharepoint_list(sharepoint_site, username, password, list_name):
    request = SharePointRequest(sharepoint_site).with_credentials(UserCredential(username, password))
    endpoint_url = f"lists/getbytitle({list_name})/items?$top=10000"
    response = request.execute_request(endpoint_url)
    return json.loads(response.content)

def get_code_type_id(code):
    """Map ContentTypeId codes to opportunity types."""
    type_mapping = {
        "0x0100A047FBF4CA286F469B7B468111C7241C010036E4AFA48A6A4142A0FF570CB64E63CF": "Oportunidad Pública",
        "0x0100A047FBF4CA286F469B7B468111C7241C00E54A1997BC3A7E4CAA92F47DB3D36F1F": "Oportunidad Privada",
        "0x0100357EF170BADC8444A735E8657AECEBA800DB007EFDD68D914B8E56BE5668F22C74": "Presentación de Capacidad"
    }
    return type_mapping.get(code, "No Informado")

def process_sharepoint_data(content):
    """Process SharePoint data and return a list of dictionaries."""
    if not content or 'd' not in content or 'results' not in content['d']:
        return []
    
    results = content['d']['results']
    list_of_dicts = []
    
    for item in results:
        try:
            # Helper function to safely get nested values
            def get_nested_value(item, key, nested_key=None, default=None):
                if key not in item or item[key] is None:
                    return default
                if nested_key:
                    return item[key].get(nested_key, default)
                return item[key]
            
            # Helper function to extract results from nested objects
            def extract_results(item, key, extract_label=False):
                if key not in item or item[key] is None:
                    return None
                if 'results' not in item[key]:
                    return None
                
                results_list = item[key]['results']
                if extract_label and results_list:
                    return [result.get('Label', '') for result in results_list]
                return results_list
            
            # Diccionario con los camps
            project_dict = {
                'Id': item.get('Id'),
                'Oportunidad': get_code_type_id(item.get('ContentTypeId')),
                'Area': item.get('AreaPyO'),
                'FechaCreacion': item.get('Created'),
                'Titulo': item.get('Title'),
                'Subarea': item.get('SubareaPyO'),
                'Sector': item.get('MercadoPyO'),
                'Cliente': item.get('ClienteNuevoPyO'),
                'Fabricantes': extract_results(item, 'FabricantePyO'),
                'IdProyecto': item.get('OTPyO'),
                'Estado': item.get('EstadoPyO'),
                'MotivoEstado': extract_results(item, 'MotivoEstado'),
                'FechaArranque': item.get('FechaArranquePyO'),
                'FechaPresentacion': item.get('FechaPresentacionPyO'),
                'Oferta': item.get('OfertaPyO'),
                'Probabilidad': item.get('ProbabilidadPyO'),
                'ResponsableMercado': extract_results(item, 'ResponsableMercadoPyOId'),
                'ResponsableArea': extract_results(item, 'ResponsableAreaPyOId'),
                'Tecnologia': extract_results(item, 'TecnologiaPyO', extract_label=True),
                'Renovacion': item.get('RenovacionPyO'),
                'Duracion': item.get('DuracionPyO'),
                'ZonaGeografica': item.get('TerritorioPyOId'),
                'Coste': item.get('CostePyO'),
                'Adjudicatario': item.get('AdjudicatarioPyO'),
                'Tipologia': item.get('TipologuaOyP'),
                'Descripción': item.get('Descripci_x00f3_nPyO'),
                'TCV/Presupuesto': item.get('TCVPresupuestoPyO'),
                'ImporteLicitado': item.get('ImporteLicitadoPyO'),
                'EmpresaOfertante': item.get('EmpresaOfertantePyO'),
                'Margen': item.get('MargenPyO'),
                'OrigenOportunidad': item.get('OrigenOportunidad')
            }
            
            list_of_dicts.append(project_dict)
            
        except Exception as e:
            print(f"Error processing item: {str(e)}")
            continue
    
    return list_of_dicts

# Main execution
try:
    # Get data from SharePoint
    print("Connecting to SharePoint...")
    content = items_from_sharepoint_list(SHAREPOINT_SITE, USERNAME, PASSWORD, SHAREPOIT_LIST)
    
    # Process the data
    print("Processing data...")
    list_of_dicts = process_sharepoint_data(content)
    
    # Convert to DataFrame
    print(f"Creating DataFrame from {len(list_of_dicts)} items...")
    df_pandas = pd.DataFrame(list_of_dicts)
    
    # Convert to Spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    df_spark = spark.createDataFrame(df_pandas)
    
    # Show the DataFrame
    print("Data loaded successfully!")
    display(df_spark)
    
    # Create a temporary view for SQL queries
    df_spark.createOrReplaceTempView("sharepoint_data")
    
    print("You can now query the data using SQL with the 'sharepoint_data' table")
    
except Exception as e:
    print(f"Error: {str(e)}")# Install required packages if not already installed
%pip install Office365-REST-Python-Client pandas requests

# Import necessary libraries
from office365.runtime.auth.user_credential import UserCredential
from pyspark.sql import SparkSession
import pandas as pd
import json
import requests

# SharePoint connection details - REPLACE THESE VALUES
SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/CRMTD"
USERNAME = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Username")
PASSWORD = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Password")
SHAREPOIT_LIST = "'Proyectos y oportunidades'"

class SharePointRequest:
    def __init__(self, site_url):
        self.site_url = site_url
        self.credentials = None
        self.auth_cookie = None
        self.digest_value = None
        
    def with_credentials(self, credentials):
        self.credentials = credentials
        return self
        
    def execute_request(self, endpoint_url):
        # Get authentication cookie
        auth_url = f"{self.site_url}/_api/contextinfo"
        auth_response = requests.post(
            auth_url,
            auth=(self.credentials.username, self.credentials.password),
            headers={'Accept': 'application/json;odata=verbose'}
        )
        
        if auth_response.status_code != 200:
            raise Exception(f"Authentication failed with status code {auth_response.status_code}: {auth_response.text}")
            
        auth_data = auth_response.json()
        self.digest_value = auth_data['d']['GetContextWebInformation']['FormDigestValue']
        
        # Execute the actual request
        api_url = f"{self.site_url}/_api/{endpoint_url}"
        headers = {
            'Accept': 'application/json;odata=verbose',
            'Content-Type': 'application/json;odata=verbose',
            'X-RequestDigest': self.digest_value
        }
        
        response = requests.get(
            api_url,
            auth=(self.credentials.username, self.credentials.password),
            headers=headers
        )
        
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
            
        return response

def items_from_sharepoint_list(sharepoint_site, username, password, list_name):
    request = SharePointRequest(sharepoint_site).with_credentials(UserCredential(username, password))
    endpoint_url = f"lists/getbytitle({list_name})/items?$top=10000"
    response = request.execute_request(endpoint_url)
    return json.loads(response.content)

def get_code_type_id(code):
    """Map ContentTypeId codes to opportunity types."""
    type_mapping = {
        "0x0100A047FBF4CA286F469B7B468111C7241C010036E4AFA48A6A4142A0FF570CB64E63CF": "Oportunidad Pública",
        "0x0100A047FBF4CA286F469B7B468111C7241C00E54A1997BC3A7E4CAA92F47DB3D36F1F": "Oportunidad Privada",
        "0x0100357EF170BADC8444A735E8657AECEBA800DB007EFDD68D914B8E56BE5668F22C74": "Presentación de Capacidad"
    }
    return type_mapping.get(code, "No Informado")

def process_sharepoint_data(content):
    """Process SharePoint data and return a list of dictionaries."""
    if not content or 'd' not in content or 'results' not in content['d']:
        return []
    
    results = content['d']['results']
    list_of_dicts = []
    
    for item in results:
        try:
            # Helper function to safely get nested values
            def get_nested_value(item, key, nested_key=None, default=None):
                if key not in item or item[key] is None:
                    return default
                if nested_key:
                    return item[key].get(nested_key, default)
                return item[key]
            
            # Helper function to extract results from nested objects
            def extract_results(item, key, extract_label=False):
                if key not in item or item[key] is None:
                    return None
                if 'results' not in item[key]:
                    return None
                
                results_list = item[key]['results']
                if extract_label and results_list:
                    return [result.get('Label', '') for result in results_list]
                return results_list
            
            # Diccionario con los camps
            project_dict = {
                'Id': item.get('Id'),
                'Oportunidad': get_code_type_id(item.get('ContentTypeId')),
                'Area': item.get('AreaPyO'),
                'FechaCreacion': item.get('Created'),
                'Titulo': item.get('Title'),
                'Subarea': item.get('SubareaPyO'),
                'Sector': item.get('MercadoPyO'),
                'Cliente': item.get('ClienteNuevoPyO'),
                'Fabricantes': extract_results(item, 'FabricantePyO'),
                'IdProyecto': item.get('OTPyO'),
                'Estado': item.get('EstadoPyO'),
                'MotivoEstado': extract_results(item, 'MotivoEstado'),
                'FechaArranque': item.get('FechaArranquePyO'),
                'FechaPresentacion': item.get('FechaPresentacionPyO'),
                'Oferta': item.get('OfertaPyO'),
                'Probabilidad': item.get('ProbabilidadPyO'),
                'ResponsableMercado': extract_results(item, 'ResponsableMercadoPyOId'),
                'ResponsableArea': extract_results(item, 'ResponsableAreaPyOId'),
                'Tecnologia': extract_results(item, 'TecnologiaPyO', extract_label=True),
                'Renovacion': item.get('RenovacionPyO'),
                'Duracion': item.get('DuracionPyO'),
                'ZonaGeografica': item.get('TerritorioPyOId'),
                'Coste': item.get('CostePyO'),
                'Adjudicatario': item.get('AdjudicatarioPyO'),
                'Tipologia': item.get('TipologuaOyP'),
                'Descripción': item.get('Descripci_x00f3_nPyO'),
                'TCV/Presupuesto': item.get('TCVPresupuestoPyO'),
                'ImporteLicitado': item.get('ImporteLicitadoPyO'),
                'EmpresaOfertante': item.get('EmpresaOfertantePyO'),
                'Margen': item.get('MargenPyO'),
                'OrigenOportunidad': item.get('OrigenOportunidad')
            }
            
            list_of_dicts.append(project_dict)
            
        except Exception as e:
            print(f"Error processing item: {str(e)}")
            continue
    
    return list_of_dicts

# Main execution
try:
    # Get data from SharePoint
    print("Connecting to SharePoint...")
    content = items_from_sharepoint_list(SHAREPOINT_SITE, USERNAME, PASSWORD, SHAREPOIT_LIST)
    
    # Process the data
    print("Processing data...")
    list_of_dicts = process_sharepoint_data(content)
    
    # Convert to DataFrame
    print(f"Creating DataFrame from {len(list_of_dicts)} items...")
    df_pandas = pd.DataFrame(list_of_dicts)
    
    # Convert to Spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    df_spark = spark.createDataFrame(df_pandas)
    
    # Show the DataFrame
    print("Data loaded successfully!")
    display(df_spark)
    
    # Create a temporary view for SQL queries
    df_spark.createOrReplaceTempView("sharepoint_data")
    
    print("You can now query the data using SQL with the 'sharepoint_data' table")
    
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS plataformas.bronze_crm.volume_crm
# MAGIC COMMENT 'Volume for the CRM sharepoint .csv';

# COMMAND ----------

import pandas as pd
from pathlib import Path
import sqlite3  # Example of SQLite for database insertion

# ===== REPLACE THESE VALUES =====
VOLUME_PATH = Path("/Volumes/plataformas/bronze_crm/volume_crm")  # Replace with your volume path
CSV_FILENAME = "Proyectos_y_oportunidades.csv"        # Replace with your CSV filename
# ===============================

# Combine volume path and filename
file_path = VOLUME_PATH / CSV_FILENAME  # Pathlib style join

def load_csv_from_volume(file_path, sep=",", encoding="utf-8"):
    """Load a CSV file from a specified path."""
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found at: {file_path}")
    
    try:
        df = pd.read_csv(file_path, sep=sep, encoding=encoding)
        print(f"Successfully loaded CSV with {df.shape[0]} rows and {df.shape[1]} columns")
        return df
    except Exception as e:
        raise Exception(f"Error loading CSV file: {str(e)}")

def format_csv_data(df, operations=None):
    """Process and format a DataFrame with common operations."""
    formatted_df = df.copy()
    
    if operations is None:
        operations = {
            'drop_na': False,
            'drop_duplicates': False,
            'lowercase_columns': False,
            'strip_strings': False,
            'datetime_columns': [],
            'numeric_columns': [],
            'rename_columns': {},
            'drop_columns': []
        }
    
    # Drop rows with NA values if specified
    if operations.get('drop_na', False):
        formatted_df = formatted_df.dropna()
    
    # Drop duplicate rows if specified
    if operations.get('drop_duplicates', False):
        formatted_df = formatted_df.drop_duplicates()
    
    # Convert column names to lowercase if specified
    if operations.get('lowercase_columns', False):
        formatted_df.columns = [col.lower() for col in formatted_df.columns]
    
    # Strip strings of leading/trailing spaces if specified
    if operations.get('strip_strings', False):
        for col in formatted_df.select_dtypes(include=['object']):
            formatted_df[col] = formatted_df[col].str.strip()
    
    # Convert specified columns to datetime format
    for col in operations.get('datetime_columns', []):
        if col in formatted_df.columns:
            formatted_df[col] = pd.to_datetime(formatted_df[col], errors='coerce')
    
    # Convert specified columns to numeric
    for col in operations.get('numeric_columns', []):
        if col in formatted_df.columns:
            formatted_df[col] = pd.to_numeric(formatted_df[col], errors='coerce')
    
    # Rename columns if specified
    if operations.get('rename_columns'):
        formatted_df = formatted_df.rename(columns=operations['rename_columns'])
    
    # Drop specified columns if needed
    if operations.get('drop_columns'):
        formatted_df = formatted_df.drop(columns=operations['drop_columns'], errors='ignore')
    
    return formatted_df

def save_to_sql(df, db_path, table_name="data_table"):
    """Save DataFrame to an SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data successfully saved to table: {table_name}")
    except Exception as e:
        print(f"Error saving data to SQL: {e}")
    finally:
        conn.close()

def save_processed_data(df, output_path):
    """Save the processed DataFrame to CSV."""
    try:
        df.to_csv(output_path, index=False)
        print(f"\nProcessed data saved to: {output_path}")
    except Exception as e:
        print(f"Error saving processed data: {e}")

# Load the CSV data
print(f"Loading CSV from: {file_path}")
df = load_csv_from_volume(file_path)

# Display basic information
print("\nData Preview:")
print(df.head())

# Process the data with common operations
# Customize these operations as needed for your specific CSV
format_operations = {
    'drop_na': True,                        # Remove rows with missing values
    'drop_duplicates': True,                # Remove duplicate rows
    'lowercase_columns': True,              # Convert column names to lowercase
    'strip_strings': True,                  # Remove extra whitespace from text
    'datetime_columns': ['date_column'],    # Add column names to convert to dates
    'numeric_columns': ['amount_column'],   # Add column names to convert to numbers
    'rename_columns': {'old_name': 'new_name'},  # Add old_name:new_name pairs to rename columns
    'drop_columns': ['unnecessary_column']  # Add names of columns to remove
}

# Format the data
formatted_df = format_csv_data(df, format_operations)

print("\nFormatted Data Preview:")
print(formatted_df.head())

# # Optionally save the processed data
# output_path = VOLUME_PATH / f"processed_{CSV_FILENAME}"
# save_processed_data(formatted_df, output_path)

# # Save to SQL table (e.g., SQLite as an example)
# db_path = VOLUME_PATH / "data.db"  # Specify your SQLite DB file path
# save_to_sql(formatted_df, db_path, table_name="proyectos_oportunidades")


# COMMAND ----------

df.display() #Igualito que mediante API
spark_df = spark.createDataFrame(df)
spark_df.createOrReplaceTempView("proyectos_oportunidades")

# COMMAND ----------

# DBTITLE 1,query tabla csv
# MAGIC %sql
# MAGIC select distinct p.IdProyecto, po.OT, po.* from proyectos_oportunidades as po
# MAGIC
# MAGIC left join operaciones.gold_unhiberse.dim_proyecto as p
# MAGIC   on p.IdProyecto = po.OT
# MAGIC where OT LIKE "%-%" and OT not LIKE "-" and p.IdProyecto IS NOT NULL
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,query de tabla api
# MAGIC %sql
# MAGIC select distinct p.IdProyecto, po.IdProyecto, po.* from plataformas.bronze_crm.lista_proyectos as po
# MAGIC
# MAGIC left join operaciones.gold_unhiberse.dim_proyecto as p
# MAGIC   on p.IdProyecto = po.IdProyecto
# MAGIC where po.IdProyecto LIKE "%-%" and po.IdProyecto not LIKE "-" and p.IdProyecto IS NOT NULL