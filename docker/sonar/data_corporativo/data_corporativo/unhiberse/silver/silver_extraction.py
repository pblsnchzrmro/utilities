# Databricks notebook source
# DBTITLE 1,Entorno
entorno = "PRE"

# COMMAND ----------

# DBTITLE 1,UDFs
# MAGIC %run "/Workspace/data_corporativo/unhiberse/utils/udfs"

# COMMAND ----------

pgSQL_dbname = "unhibersedaily"
pgSQL_url = f"jdbc:postgresql://{pgSQL_host}:{pgSQL_port}/{pgSQL_dbname}"

# COMMAND ----------

# DBTITLE 1,Utils
# MAGIC %run "/Workspace/data_corporativo/unhiberse/utils/utils"

# COMMAND ----------

# DBTITLE 1,Leer tablas de Unhiberse
df_tablas = read_pgSQL_data("information_schema.tables").select('`table_name`').filter("table_type = 'BASE TABLE'").filter(~col('table_name').like('mte_%'))
df_tablas.display()

# COMMAND ----------

# DBTITLE 1,Schema
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS operaciones.silver_unhiberse
""")

# COMMAND ----------

# DBTITLE 1,Tablas necesarias
# Array con los nombres de las tablas silver 
table_names = ["arabia_client_type",
    "category_market",
    "client",
    "client_group",
    "client_type",
    "expense",
    "expense_type",
    "incurrence",
    "incurrence_item",
    "incurrence_state_type",
    "invoice",
    "invoice_category_type",
    "invoice_item",
    "invoice_state_type",
    "payment_condition",
    "period",
    "project",
    "project_activity_type",
    "project_billing_type",
    "project_hour",
    "project_state",
    "project_type",
    "ue_group", 
    "client_group_client", 
    "project_secondary_parent"
]

# COMMAND ----------

# DBTITLE 1,Cargar tablas
# Diccionario para almacenar los dataframes con el nombre de la tabla como clave
dataframes = {}

# Bucle para recorrer las tablas y llamar a la función de lectura
for table in table_names:
    # Llamamos a la función para leer los datos
    df = read_pgSQL_data(table)
    
    dataframes[table] = df

    # Verificar si el dataframe existe en el diccionario
    if table in dataframes:
        # Guardar el dataframe como una tabla en el esquema operaciones.silver_unhiberse
        dataframes[table].write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"operaciones.silver_unhiberse.{table}")
        
        # Crear una vista temporal si es necesario para consultas en el notebook
        dataframes[table].createOrReplaceTempView(table)
        
        print(f"Table '{table}' created and saved in the catalog.")
    else:
        print(f"Warning: DataFrame for table '{table}' not found.")

# COMMAND ----------

# DBTITLE 1,Tabla auxiliar
# Esta tabla no se encuentra en Unhiberse, entonces creamos tabla mercado_auxiliar llamando a tablas de operaciones.bronze_mte.
query = """
SELECT 
  hma.Nombre,
  hma.ID,
  hbu.Nombre as Nombre_hbu,
  hbu.ID as id_hbu,
  hbu.hMA  as hMA_hbu

FROM operaciones.bronze_mte.hma

LEFT JOIN operaciones.bronze_mte.hbu
  on hma.Nombre = hbu.hMA

WHERE hma.EsMercado = true

AND hbu.EsSector = true
"""

# Cargamos a dataframe
df = spark.sql(query)

# Guardar el dataframe como una tabla en el esquema operaciones.silver_unhiberse
df.write.format("delta").mode("overwrite").saveAsTable("operaciones.silver_unhiberse.mercado_auxiliar")

