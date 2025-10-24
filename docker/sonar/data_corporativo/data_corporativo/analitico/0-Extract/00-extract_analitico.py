# Databricks notebook source
# MAGIC %md
# MAGIC This Notebook downloads data to DBFS.

# COMMAND ----------

# DBTITLE 1,Importing Sharepoint DB libs
# MAGIC %run "/Workspace/data_corporativo/analitico/utils/01Utils"

# COMMAND ----------

# MAGIC %md
# MAGIC Edita los parámetros después del "else" para descargar los ficheros que consideres necesario manualmente.

# COMMAND ----------

# DBTITLE 1,ENV properties. Select True if running with WF. False if running manually
if len(dbutils.widgets.getAll().items()) != 0:
    carga_Axapta = dbutils.widgets.get("carga_Axapta")
    carga_B_Año_Act = dbutils.widgets.get("carga_B_Anyo_Act")
    carga_B_Año_Ant = dbutils.widgets.get("carga_B_Anyo_Ant")
    carga_Epsilon = dbutils.widgets.get("carga_Epsilon")
    carga_incentivos = dbutils.widgets.get("carga_incentivos")
    carga_asientos = dbutils.widgets.get("carga_asientos")
    carga_Horas_KVP = dbutils.widgets.get("carga_Horas_KVP")
    carga_obra_año_act = dbutils.widgets.get("carga_obra_anyo_act")
    carga_obra_año_anterior = dbutils.widgets.get("carga_obra_anyo_anterior")
    carga_tablas = dbutils.widgets.get("carga_tablas")
    carga_datos_historicos = dbutils.widgets.get("carga_datos_historicos")
    running_date = datetime.datetime.strptime(dbutils.widgets.get("running_load_date"), '%Y-%m-%d').date()
    SHAREPOINT_SITE = dbutils.widgets.get("sharepoint_site")
    SHAREPOINT_SITE_NAME = dbutils.widgets.get("sharepoint_site_name")
    SHAREPOINT_DOC = dbutils.widgets.get("sharepoint_doc")
else:
    carga_Axapta = "Y"
    carga_B_Año_Act = "N"
    carga_B_Año_Ant = "N"
    carga_Epsilon = "N"
    carga_Horas_KVP = "N"
    carga_obra_año_act = "N"
    carga_obra_año_anterior = "N"
    carga_incentivos = "N"
    carga_asientos = "N"
    carga_tablas = "N"
    carga_datos_historicos = "N"
    running_date = datetime.datetime.today()

previous_month = running_date + dateutil.relativedelta.relativedelta(months=N_PREVIOUS_MONTHS)
mes_carga = '{:02d}'.format(previous_month.month) 
anyo_carga = str(previous_month.year)
initial_prefix = 'CONSOL'
KEYWORD = f'{mes_carga}{anyo_carga}'

# COMMAND ----------

# MAGIC %md
# MAGIC ### File definition
# MAGIC In case you need to define more files, you can define them from here.
# MAGIC
# MAGIC This is the structure:
# MAGIC - **Load boolean**: Indicates if it should be loaded or not.
# MAGIC - **The name of the folder in Sharepoint**.
# MAGIC - **The name of the folder in local file system**.
# MAGIC - **Multiple sheet**: True if it stores each company in different folders. False if it stores in a single folder.
# MAGIC - **Special Folder boolean**. True if it's a special folder that doesn't follow structure of YEAR folder. It only works if _Multiple sheet_ is False.

# COMMAND ----------

# DBTITLE 1,Files definition.
log_definition = [
    (carga_Axapta, 'Transacciones AX', 'Transacciones AX', True, False),
    (carga_B_Año_Act, 'Bonus', 'Bonus_Act', False, False),
    (carga_B_Año_Ant, 'Bonus año anterior', 'Bonus_Ant', False, False),
    (carga_Horas_KVP, 'Horas (Kvp)', 'Horas_KVP', True, False),
    (carga_Epsilon, 'RRHH (Epsilon)', 'RRHH_Epsilon', False, False),
    (carga_obra_año_anterior, 'Obra en curso año anterior', 'Obra_curso_año_anterior', True, False),
    (carga_obra_año_act, 'Obra en curso año actual', 'Obra_curso_año_actual', True, False),
    # (carga_facturacion_interna, 'Facturación interna', 'Facturacion_interna', False, False),
    (carga_incentivos, 'Incentivos', 'Incentivos', False, False),
    (carga_asientos, 'Nombre Asientos', 'Nombre Asientos', False, True),
    (carga_tablas, 'Tablas', 'Tablas', False, True)
]

# COMMAND ----------

for component in log_definition:
    download_component(component)

# COMMAND ----------

# DBTITLE 1,Download historical data
if carga_datos_historicos == "Y":
    FOLDER_NAME = f'{initial_prefix}/'
    FOLDER_DEST = f"{mounting_point}/historico_transacciones/"
    KEYWORD = 'transacciones'
    download_files_shp_pattern(KEYWORD, FOLDER_NAME, FOLDER_DEST)