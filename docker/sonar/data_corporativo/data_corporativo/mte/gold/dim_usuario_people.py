# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.silver_mte.view_usuarios")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Column names normalizados

# Selección columnas para vista final Gold
df = df.select('NombreCompleto', 'ID','EmailCorporativo','CodigoProyecto','CodigoProyectoVAC', 'CodigoProyectoPER','Imputa','AppImputacion','Productividad','Edad', 'RangoEdad','AñosAntiguedad','RangoAntiguedad','Sexo','PuestoTrabajo', 'ModoTrabajo','hMA','hBU','SubhBU','EquipoTrabajohBU','EquipoTrabajoMultiarea','Bonificable','Reasignable','EsBaja', 'FECHA_CARGA')

# Rename the column 'id' to 'idUsuario'
df = df.withColumnRenamed('id', 'IdUsuario').withColumn('IdUsuario', F.col('IdUsuario').cast('int'))

df = df.withColumn('NombreCompleto', F.initcap(F.col('NombreCompleto')))

df = df.withColumnRenamed('FECHA_CARGA', 'FechaCarga').withColumnRenamed('Antiguedad', 'Antigüedad').withColumnRenamed('RangoAntiguedad', 'RangoAntigüedad')


df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.dim_usuario')



# COMMAND ----------

# df = spark.read.table("operaciones.gold_mte.dim_empresas_hist")

# df = df.withColumnRenamed('ID', 'IdEmpresa').withColumnRenamed('Nombre', 'Empresa').withColumnRenamed('FECHA_CARGA', 'FechaCarga')

# df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.dim_empresas_hist')

# COMMAND ----------

# from datetime import datetime, timedelta
# from delta.tables import DeltaTable

# # Check if today is the last day of the current month
# today = datetime.today()
# first_day_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
# is_last_day_of_month = today == first_day_next_month - timedelta(days=1)

# if is_last_day_of_month:

#     # MERGE HIST TABLE

#     target_table = 'operaciones.gold_mte.dim_usuario_hist'

#     # Drop column Fecha_carga
#     # df = df.drop('FECHA_CARGA')

#     # Check if the target table exists
#     if not spark.catalog.tableExists(target_table):
#         # Create the target table with the schema of the source DataFrame
#         df.write.format("delta").saveAsTable(target_table)

#     # Load the target table into a DeltaTable
#     target_table = DeltaTable.forName(spark,target_table)

#     # Drop column Fecha_carga
#     # df = df.drop('FECHA_CARGA')

#     # Perform the merge operation
#     (target_table.alias('target')
#     .merge(df.alias('source'), 'source.IdUsuario = target.IdUsuario AND source.EmailCorporativo = target.EmailCorporativo AND source.FechaCarga = target.FechaCarga')
#     .whenMatchedUpdateAll()
#     .whenNotMatchedInsertAll()
#     .execute())