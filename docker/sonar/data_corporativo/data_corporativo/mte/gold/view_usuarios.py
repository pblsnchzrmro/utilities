# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.silver_mte.view_usuarios")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

# Rename the column 'id' to 'idUsuario'
df = df.withColumnRenamed('id', 'IdUsuario').withColumn('IdUsuario',F.col('IdUsuario').cast('int'))

# Columna NombreCompleto initcap
df = df.withColumn('NombreCompleto', F.initcap(F.trim(F.col('NombreCompleto'))))

# FechasAlta y FechasBaja
df = df.filter(df['FechaAlta'].isNotNull())
df = df.withColumn('FechaBaja',F.to_date(F.when(df['FechaBaja'].isNull(), F.lit('9999-12-31')).otherwise(df['FechaBaja']),'yyyy-MM-dd'))

df = df.drop("Direccion").drop("Bajas").drop("ResponsablehBU").drop("ResponsablehMA").drop("ResponsableEquipoTrabajoMultiarea").drop("AprobadorVacaciones").drop("CodigoProyecto")
df = df.withColumnRenamed("ComunidadesTecnologicas", "ComTecnologica")

df.display()

# COMMAND ----------

# Databricks notebook
from pyspark.sql import functions as F

# First get all the responsible names from the dimension tables
# 1. Get responsible names for hBU
hbu_responsibles = spark.sql("""
SELECT 
    hbu.IdhBU,
    hbu.Responsable as ResponsablehBU
FROM operaciones.gold_mte.dim_hbu hbu
WHERE hbu.Responsable IS NOT NULL AND trim(hbu.Responsable) != ''
""")

# 2. Get responsible names for hMA
hma_responsibles = spark.sql("""
SELECT 
    hma.IdhMA,
    hma.Responsable as ResponsablehMA
FROM operaciones.gold_mte.dim_hma hma
WHERE hma.Responsable IS NOT NULL AND trim(hma.Responsable) != ''
""")

# 3. Get responsible names for Equipo Multiarea
equipo_multiarea_responsibles = spark.sql("""
SELECT 
    etm.IdEquipoMultitarea,
    etm.Responsable as ResponsableEquipoTrabajoMultiarea
FROM operaciones.gold_mte.dim_equipostrabajomultiarea etm
WHERE etm.Responsable IS NOT NULL AND trim(etm.Responsable) != ''
""")

# 4. Get responsible names for SubhBU
subhbu_responsibles = spark.sql("""
SELECT 
    shbu.IdSubhBU,
    shbu.Responsable as ResponsableSubhBU
FROM operaciones.gold_mte.dim_subhbu shbu
WHERE shbu.Responsable IS NOT NULL AND trim(shbu.Responsable) != ''
""")

# 5. Get responsible names for hGZ
hgz_responsibles = spark.sql("""
SELECT 
    hgz.IdhGZ,
    hgz.Responsable as ResponsablehGZ
FROM operaciones.gold_mte.dim_hgz hgz
WHERE hgz.Responsable IS NOT NULL AND trim(hgz.Responsable) != ''
""")

# 6. Get responsible names for hGR
hgr_responsibles = spark.sql("""
SELECT 
    hgr.IdhGR,
    hgr.Responsable as ResponsablehGR
FROM operaciones.gold_mte.dim_hgr hgr
WHERE hgr.Responsable IS NOT NULL AND trim(hgr.Responsable) != ''
""")

# 7. Get responsible names for SubhGR
subhgr_responsibles = spark.sql("""
SELECT 
    shgr.IdSubhGR,
    shgr.Responsable as ResponsableSubhGR
FROM operaciones.gold_mte.dim_subhgr shgr
WHERE shgr.Responsable IS NOT NULL AND trim(shgr.Responsable) != ''
""")

# 8. Get responsible names for Equipo hBU
equipo_hbu_responsibles = spark.sql("""
SELECT 
    ethbu.IdEquipohBU,
    ethbu.Responsable as ResponsableEquipoTrabajohBU
FROM operaciones.gold_mte.dim_equipostrabajohbu ethbu
WHERE ethbu.Responsable IS NOT NULL AND trim(ethbu.Responsable) != ''
""")

# 9. Get vacation approvers
vacation_approvers = spark.sql("""
SELECT 
    uia.ID as Idusuario,
    uia.AprobadorVacaciones
FROM people.gold_mteia.usuarios_ia uia
WHERE uia.AprobadorVacaciones IS NOT NULL AND trim(uia.AprobadorVacaciones) != ''
""")

# Now we need to get the users and their field assignments
# Let's assume there's a user_assignments table that maps users to fields
# If such table doesn't exist, you'll need to create these mappings based on your data model

# Example of how to get user-to-field mappings - modify according to your actual data structure
# Assuming fact_usuarios is your user table - adjust this to your actual table name
user_hbu_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdhBU
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdhBU IS NOT NULL
""")

user_hma_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdhMA
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdhMA IS NOT NULL
""")

user_equipomultiarea_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdEquipoTrabajoMultiarea
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdEquipoTrabajoMultiarea IS NOT NULL
""")

user_subhbu_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdSubhBU
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdSubhBU IS NOT NULL
""")

user_hgz_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdhGZ
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdhGZ IS NOT NULL
""")

user_hgr_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdhGR
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdhGR IS NOT NULL
""")

user_subhgr_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdSubhGR
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdSubhGR IS NOT NULL
""")

user_equipohbu_mapping = spark.sql("""
SELECT 
    u.ID as Idusuario,
    u.IdEquipoTrabajohBU
FROM people.bronze_mte.fact_usuarios u
WHERE u.IdEquipoTrabajohBU IS NOT NULL
""")

# Now join the field responsibles with user field mappings to get user-to-responsible mappings

# 1. hBU responsibles by user
user_hbu_responsibles = user_hbu_mapping.join(
    hbu_responsibles,
    on="IdhBU",
    how="left"
).select("Idusuario", "ResponsablehBU")

# 2. hMA responsibles by user
user_hma_responsibles = user_hma_mapping.join(
    hma_responsibles,
    on="IdhMA",
    how="left"
).select("Idusuario", "ResponsablehMA")

# 3. Equipo Multiarea responsibles by user
user_equipomultiarea_responsibles = user_equipomultiarea_mapping.join(
    equipo_multiarea_responsibles,
    user_equipomultiarea_mapping["IdEquipoTrabajoMultiarea"] == equipo_multiarea_responsibles["IdEquipoMultitarea"],
    how="left"
).select("Idusuario", "ResponsableEquipoTrabajoMultiarea")

# 4. SubhBU responsibles by user
user_subhbu_responsibles = user_subhbu_mapping.join(
    subhbu_responsibles,
    on="IdSubhBU",
    how="left"
).select("Idusuario", "ResponsableSubhBU")

# 5. hGZ responsibles by user
user_hgz_responsibles = user_hgz_mapping.join(
    hgz_responsibles,
    on="IdhGZ",
    how="left"
).select("Idusuario", "ResponsablehGZ")

# 6. hGR responsibles by user
user_hgr_responsibles = user_hgr_mapping.join(
    hgr_responsibles,
    on="IdhGR",
    how="left"
).select("Idusuario", "ResponsablehGR")

# 7. SubhGR responsibles by user
user_subhgr_responsibles = user_subhgr_mapping.join(
    subhgr_responsibles,
    on="IdSubhGR",
    how="left"
).select("Idusuario", "ResponsableSubhGR")

# 8. Equipo hBU responsibles by user
user_equipohbu_responsibles = user_equipohbu_mapping.join(
    equipo_hbu_responsibles,
    user_equipohbu_mapping["IdEquipoTrabajohBU"] == equipo_hbu_responsibles["IdEquipohBU"],
    how="left"
).select("Idusuario", "ResponsableEquipoTrabajohBU")



# COMMAND ----------


df = df \
    .join(user_hbu_responsibles, on="Idusuario", how="left") \
    .join(user_hma_responsibles, on="Idusuario", how="left") \
    .join(user_equipomultiarea_responsibles, on="Idusuario", how="left") \
    .join(user_subhbu_responsibles, on="Idusuario", how="left") \
    .join(user_hgz_responsibles, on="Idusuario", how="left") \
    .join(user_hgr_responsibles, on="Idusuario", how="left") \
    .join(user_subhgr_responsibles, on="Idusuario", how="left") \
    .join(user_equipohbu_responsibles, on="Idusuario", how="left") \
    .join(vacation_approvers, on="Idusuario", how="left")

# Display the resulting dataframe with all added columns
df.display()


# COMMAND ----------

from pyspark.sql import functions as F

# More efficient way to identify columns with at least one non-null value
# This approach uses a single pass through the data rather than checking each column separately
non_null_counts = df.select([F.count(F.when(F.col(c).isNotNull(), c)).alias(c) for c in df.columns]).first()

# Filter columns that have at least one non-null value
non_null_columns = [c for c in df.columns if non_null_counts[c] > 0]

# Select only columns with at least one non-null value
df_filtered = df.select(*non_null_columns)

# Display the filtered DataFrame with non-null columns only
df_filtered.display()

# Optional: Print how many columns were removed
removed_columns = [c for c in df.columns if c not in non_null_columns]
removed_columns_df = spark.createDataFrame([(col,) for col in removed_columns], ["Removed Columns"])
removed_columns_df.display()

# COMMAND ----------

from pyspark.sql.functions import to_date

# Cast FECHA_CARGA column from string to date yyyy-mm-dd
df = df.withColumn('FECHA_CARGA', to_date(F.col('FECHA_CARGA'), 'yyyy-MM-dd'))
df = df.withColumnRenamed('FECHA_CARGA', 'Fechacarga')

df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.view_usuarios')


# COMMAND ----------

from datetime import datetime, timedelta
from delta.tables import DeltaTable

# Check if today is the last day of the current month
today = datetime.today()
first_day_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
is_last_day_of_month = today == first_day_next_month - timedelta(days=1)

if is_last_day_of_month:

    
    # MERGE HIST TABLE

    target_table = 'people.gold_mte.view_usuarios_hist'

    # Drop column Fecha_carga
    # df = df.drop('Fechacarga')

    # Check if the target table exists
    if not spark.catalog.tableExists(target_table):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table)

    # Load the target table into a DeltaTable
    target_table = DeltaTable.forName(spark,target_table)

    # Drop column Fecha_carga
    # df = df.drop('Fechacarga')

    # Perform the merge operation
    (target_table.alias('target')
    .merge(df.alias('source'), 'source.IdUsuario = target.IdUsuario AND source.nombreCompleto = target.nombreCompleto AND source.Fechacarga = target.Fechacarga')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())