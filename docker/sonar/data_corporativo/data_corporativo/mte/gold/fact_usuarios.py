# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.silver_mte.fact_usuarios")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Columna NombreCompleto initcap
df = df.withColumn('NombreCompleto', F.initcap(F.trim(F.col('NombreCompleto'))))

# Rename the column 'id' to 'idUsuario'
df = df.withColumnRenamed('id', 'IdUsuario')
df = df.withColumn('IdUsuario', F.col('IdUsuario').cast('int'))
df = df.withColumnRenamed('FECHA_CARGA', 'FechaCarga')
df = df.withColumnRenamed('IdComunidadesTecnologicas', 'IdComTec')

# Quitar TarifaCoste
df = df.drop("TarifaCoste").drop("DNI").drop("EmailPersonal").drop("FechaNacimiento").drop("TelefonoPersonal").drop("Bajas").drop("Edad").drop("GrupoNivel").drop("NivelProfesional").drop("CarreraProfesional").drop("IdComTec").drop("Direccion").drop("CodigoProyecto").drop("FechaUltimoCambioSalarial").drop("FranjaSalarialActual").drop("FranjaSalarialAnterior").drop("IdNIvelActingAs").drop("PorcentajeIncrementoSalarial").drop("FechaUltimaRevisionSalarial").drop("IdTitulación").drop("IdResponsablehBU").drop("IdResponsablehMA").drop("IdResponsableEquipoTrabajoMultiarea").drop("IdAprobadorVacaciones").drop("ResponsablehBU").drop("ResponsablehMA").drop("ResponsableEquipoTrabajoMultiarea").drop("AprobadorVacaciones")

df = df.filter(df['FechaAlta'].isNotNull())
df = df.withColumn('FechaBaja',F.to_date(F.when(df['FechaBaja'].isNull(), F.lit('9999-12-31')).otherwise(df['FechaBaja']),'yyyy-MM-dd'))



df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.fact_usuarios')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.fact_usuarios')

# COMMAND ----------

from datetime import datetime, timedelta
from delta.tables import DeltaTable

# Check if today is the last day of the current month
today = datetime.today()
first_day_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
is_last_day_of_month = today == first_day_next_month - timedelta(days=1)

if is_last_day_of_month:

    # Check if the target table exists
    target_table = 'operaciones.gold_mte.fact_usuarios_hist'
    if not spark.catalog.tableExists(target_table):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table)

    # Load the target table into a DeltaTable
    target_table = DeltaTable.forName(spark, target_table)

    # Perform the merge operation
    (target_table.alias('target')
     .merge(df.alias('source'), 'source.IdUsuario = target.IdUsuario AND source.nombreCompleto = target.nombreCompleto AND source.FECHA_CARGA = target.FECHA_CARGA')
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

    # Check if the target table exists in the 'people' catalog
    target_table_people = 'people.gold_mte.fact_usuarios_hist'
    if not spark.catalog.tableExists(target_table_people):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table_people)

    # Load the target table into a DeltaTable
    target_table_people = DeltaTable.forName(spark, target_table_people)

    # Perform the merge operation
    (target_table_people.alias('target')
     .merge(df.alias('source'), 'source.IdUsuario = target.IdUsuario AND source.nombreCompleto = target.nombreCompleto AND source.FECHA_CARGA = target.FECHA_CARGA')
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())