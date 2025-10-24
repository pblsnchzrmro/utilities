# Databricks notebook source
# MAGIC %pip install Office365-REST-Python-Client
# MAGIC
# MAGIC from office365.runtime.auth.authentication_context import AuthenticationContext
# MAGIC from office365.sharepoint.client_context import ClientContext
# MAGIC from office365.sharepoint.files.file import File 
# MAGIC
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC import json
# MAGIC from office365.runtime.auth.user_credential import UserCredential
# MAGIC from office365.sharepoint.request import SharePointRequest
# MAGIC import re
# MAGIC import datetime
# MAGIC import calendar
# MAGIC import pandas as pd
# MAGIC import seaborn as sns

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS plataformas.bronze_crm")

# COMMAND ----------

SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/CRMTD"
SHAREPOIT_LIST = "'Proyectos y oportunidades'"

# COMMAND ----------

# Secretos
USERNAME = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Username")
PASSWORD = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "SharePoint-Password")

# COMMAND ----------

scopes = dbutils.secrets.list(scope="KeyVaultDataMTC")

# Display all available scopes
for key in scopes:
    print(key.key)

# COMMAND ----------

def items_from_sharepoint_list(sharepoint_site, username, password, list_name):
    request = SharePointRequest(sharepoint_site).with_credentials(UserCredential(username, password))
    endpoint_url = f"lists/getbytitle({list_name})/items?$top=20000"
    response = request.execute_request(endpoint_url)
    return json.loads(response.content) 


# COMMAND ----------

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


content = items_from_sharepoint_list(SHAREPOINT_SITE, USERNAME, PASSWORD, SHAREPOIT_LIST)
list_of_dicts = process_sharepoint_data(content)


# COMMAND ----------

df = pd.DataFrame(list_of_dicts) # Pandas DataFrame

# COMMAND ----------

# Pandas DataFrame, convert it to a Spark DataFrame first
spark_df = spark.createDataFrame(df)

# COMMAND ----------

#spark_df.display() #responsables de mercado 1022 y 1055 posible fallo de escribir 102 y 105

# COMMAND ----------

# Write the Spark DataFrame to a Delta table
spark_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.bronze_crm.lista_proyectos")

# COMMAND ----------

# %sql
# drop table if exists plataformas.bronze_crm.lista_proyectos

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from plataformas.bronze_crm.lista_proyectos as lp
# MAGIC left join operaciones.gold_unhiberse.dim_proyecto as p
# MAGIC   on p.IdProyecto = lp.IdProyecto
# MAGIC where p.IdProyecto is not null
# MAGIC and lp.FechaPresentacion >= '2024-01-01'
# MAGIC and p.TipoProyecto LIKE '%ffering'