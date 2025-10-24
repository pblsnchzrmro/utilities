# Databricks notebook source
# Read table from catalog
df = spark.read.table("operaciones.silver_mte.Empresas")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Column names normalizados

# Selección columnas para vista final Gold
df = df.select('ID','Nombre','Consolidada', 'Activo', 'ActivoUnhiberse','hMAHabilitadas', 'Responsable', 'ResponsableEmail', 'FECHA_CARGA')

df.display()

# COMMAND ----------

df = df.withColumnRenamed('ID', 'IdEmpresa').withColumnRenamed('Nombre', 'Empresa').withColumnRenamed('FECHA_CARGA', 'FechaCarga')

df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.dim_Empresas')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.dim_Empresas')

# COMMAND ----------

# df = spark.read.table("operaciones.gold_mte.dim_empresas_hist")

# df = df.withColumnRenamed('ID', 'IdEmpresa').withColumnRenamed('Nombre', 'Empresa').withColumnRenamed('FECHA_CARGA', 'FechaCarga')

# df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.dim_empresas_hist')

# COMMAND ----------

from datetime import datetime, timedelta
from delta.tables import DeltaTable

# Check if today is the last day of the current month
today = datetime.today()
first_day_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
is_last_day_of_month = today == first_day_next_month - timedelta(days=1)

if is_last_day_of_month:

    # MERGE HIST TABLE

    target_table = 'operaciones.gold_mte.dim_Empresas_HIST'

    # Check if the target table exists
    if not spark.catalog.tableExists(target_table):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table)

    # Load the target table into a DeltaTable
    target_table = DeltaTable.forName(spark, target_table)

    # Perform the merge operation
    (target_table.alias('target')
    .merge(df.alias('source'), 'source.IdEmpresa = target.IdEmpresa AND source.Empresa = target.Empresa AND source.FechaCarga = target.FechaCarga')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

    # MERGE HIST TABLE for PEOPLE catalog

    target_table_people = 'people.gold_mte.dim_Empresas_HIST'

    # Check if the target table exists
    if not spark.catalog.tableExists(target_table_people):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table_people)

    # Load the target table into a DeltaTable
    target_table_people = DeltaTable.forName(spark, target_table_people)

    # Perform the merge operation
    (target_table_people.alias('target')
    .merge(df.alias('source'), 'source.IdEmpresa = target.IdEmpresa AND source.Empresa = target.Empresa AND source.FechaCarga = target.FechaCarga')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())