# Databricks notebook source
import requests
import json
from pyspark.sql.functions import col, concat_ws, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, DoubleType, IntegerType

# COMMAND ----------

# MAGIC %run /Workspace/data_corporativo/mte/utils/common

# COMMAND ----------

token_formatted = API_MTE_token()
url = "https://mte-produccion.azure-api.net/data/v1/USR/Items"
data = request_data(url, token_formatted)
print(data)

# COMMAND ----------

schema = StructType([
    StructField("NombreCompleto", StringType(), True),
    StructField("DNI", StringType(), True),
    StructField("EmailCorporativo", StringType(), True),
    StructField("EmailPersonal", StringType(), True),
    StructField("AppImputacion", StringType(), True),
    StructField("CarreraProfesional", StructType([ 
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("CentroTrabajo", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("CodigoProyecto", StringType(), True),
    StructField("CodigoProyectoPER", StringType(), True),
    StructField("Reasignable", StringType(), True), 
    StructField("CodigoProyectoVAC", StringType(), True),
    StructField("Direccion", StringType(), True),
    StructField("Empresa", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),

    StructField("EquipoTrabajoMultiarea", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True),
        StructField("Responsable", ArrayType(StructType([
            StructField("Id", IntegerType(), True),
            StructField("Value", StringType(), True),
            StructField("Email", StringType(), True)
        ])), True)
    ]), True),
    StructField("Productividad", StringType(), True), 
    StructField("Bonificable", StringType(), True), 
    StructField("EquipoTrabajohBU", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("FechaAlta", StringType(), True),
    StructField("FechaAntiguedad", StringType(), True),
    StructField("FechaBaja", StringType(), True),
    StructField("FechaNacimiento", StringType(), True),
    StructField("FechaUltimoCambioSalarial", StringType(), True),
    StructField("FranjaSalarialActual", StringType(), True),
    StructField("FranjaSalarialAnterior", StringType(), True),
    StructField("GrupoNivel", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("ID", StringType(), True),
    StructField("Imputa", StringType(), True),
    StructField("ModoTrabajo", StringType(), True),
    StructField("NivelActingAs", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("NivelConsolidado", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("NivelProfesional", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("PorcentajeIncrementoSalarial", DoubleType(), True),
    StructField("ProvieneDe", StringType(), True),
    StructField("PuestoTrabajo", StringType(), True),
    StructField("Recruiter", StringType(), True),
    StructField("TarifaCoste", StringType(), True), 
    StructField("FechaUltimaRevisionSalarial", StringType(), True), 
    StructField("TipoContrato", StringType(), True),
    StructField("Jornada", DoubleType(), True), 
    StructField("Sexo", StringType(), True),
    StructField("SubhBU", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("SubhGR", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("TelefonoPersonal", StringType(), True),
    StructField("TipoPerfil", StringType(), True),
    StructField("Titulacion", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("ComunidadesTecnologicas", ArrayType(StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ])), True),
    StructField("Usuario", StructType([
        StructField("Email", StringType(), True)
    ]), True),
    StructField("hBU", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True),
        StructField("Responsable", ArrayType(StructType([
            StructField("Id", IntegerType(), True),
            StructField("Value", StringType(), True),
            StructField("Email", StringType(), True)
        ])), True)
    ]), True),

    StructField("hGR", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("hGZ", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True)
    ]), True),
    StructField("hMA", StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True),
        StructField("Responsable", ArrayType(StructType([
            StructField("Id", IntegerType(), True),
            StructField("Value", StringType(), True),
            StructField("Email", StringType(), True)
        ])), True)
    ]), True),

    StructField("Bajas", ArrayType(StringType()), True),
    StructField("Aprobador", StructType([
        StructField("aprobador", StructType([
            StructField("Id", IntegerType(), True),
            StructField("Value", StringType(), True)
        ]), True)
    ]), True)
])

mte_df = spark.createDataFrame(data, schema=schema)
# mte_df = spark.createDataFrame(data)
# mte_df.display()

# COMMAND ----------

mte_formatted = mte_df.select( \
    col("NombreCompleto"), \
    col("DNI"), \
    col("EmailCorporativo"), \
    col("EmailPersonal"), \
    col("AppImputacion"), \
    col("CarreraProfesional")["Id"].alias("IdCarreraProfesional"), \
    col("CarreraProfesional")["Value"].alias("CarreraProfesional"), \
    col("CentroTrabajo")["Id"].alias("IdCentroTrabajo"), \
    col("CodigoProyecto"), \
    col("CodigoProyectoPER"), \
    col("Reasignable"), \
    col("CodigoProyectoVAC"), \
    col("Direccion"), \
    col("Empresa")["Id"].alias("IdEmpresa"), \
    col("EquipoTrabajoMultiarea")["Id"].alias("IdEquipoTrabajoMultiarea"), \
    col("Productividad"), \
    col("Bonificable"), \
    col("EquipoTrabajohBU")["Id"].alias("IdEquipoTrabajohBU"), \
    col("FechaAlta"), \
    col("FechaAntiguedad"), \
    col("FechaBaja"), \
    col("FechaNacimiento"), \
    col("FechaUltimoCambioSalarial"), \
    col("FranjaSalarialActual"), \
    col("FranjaSalarialAnterior"), \
    col("GrupoNivel")["Id"].alias("IdGrupoNivel"), \
    col("GrupoNivel")["Value"].alias("GrupoNivel"), \
    col("ID"), \
    col("Imputa"), \
    col("ModoTrabajo"), \
    col("NivelActingAs")["Id"].alias("IdNivelActingAs"), \
    col("NivelConsolidado")["Id"].alias("IdNivelConsolidado"), \
    col("NivelProfesional")["Id"].alias("IdNivelProfesional"), \
    col("NivelProfesional")["Value"].alias("NivelProfesional"), 
    col("PorcentajeIncrementoSalarial"), \
    col("ProvieneDe"), \
    col("PuestoTrabajo"), \
    col("Recruiter"), \
    col("TarifaCoste"), \
    col("FechaUltimaRevisionSalarial"), \
    col("TipoContrato"), \
    col("Jornada"), \
    col("Sexo"), \
    col("SubhBU")["Id"].alias("IdSubhBU"), \
    col("SubhGR")["Id"].alias("IdSubhGR"), \
    col("TelefonoPersonal"), \
    col("TipoPerfil"), \
    col("Titulacion")["Id"].alias("IdTitulacion"), \
    col("Titulacion")["Value"].alias("Titulacion"), \
    concat_ws(", ", expr("TRANSFORM(ComunidadesTecnologicas, x -> x.Id)")).alias("IdComunidadesTecnologicas"), \
    concat_ws(", ", expr("TRANSFORM(ComunidadesTecnologicas, x -> x.Value)")).alias("ComunidadesTecnologicas"), \
    col("Usuario")["Email"].alias("Usuario"), \
    col("hBU")["Id"].alias("IdhBU"), \
    col("hGR")["Id"].alias("IdhGR"), \
    col("hGZ")["Id"].alias("IdhGZ"), \
    col("hMA")["Id"].alias("IdhMA"), \
    col("Bajas"), \
    concat_ws(", ", expr("TRANSFORM(hBU.Responsable, x -> x.Id)")).alias("IdResponsablehBU"), \
    concat_ws(", ", expr("TRANSFORM(hMA.Responsable, x -> x.Id)")).alias("IdResponsablehMA"), \
    concat_ws(", ", expr("TRANSFORM(EquipoTrabajoMultiarea.Responsable, x -> x.Id)")).alias("IdResponsableEquipoTrabajoMultiarea"), \
    col("Aprobador")["aprobador"]['Id'].alias("IdAprobadorVacaciones"), \
    concat_ws(", ", expr("TRANSFORM(hBU.Responsable, x -> x.Value)")).alias("ResponsablehBU"), \
    concat_ws(", ", expr("TRANSFORM(hMA.Responsable, x -> x.Value)")).alias("ResponsablehMA"), \
    concat_ws(", ", expr("TRANSFORM(EquipoTrabajoMultiarea.Responsable, x -> x.Value)")).alias("ResponsableEquipoTrabajoMultiarea"), \
    col("Aprobador")["aprobador"]['value'].alias("AprobadorVacaciones"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, to_date

# Add current date column in 'dd-MM-yyyy' format
mte_formatted = mte_formatted.withColumn("FECHA_CARGA",current_timestamp())

# COMMAND ----------

# mte_formatted.display()

# COMMAND ----------

mte_formatted.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.bronze_mte.fact_usuarios')
mte_formatted.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.bronze_mte.fact_usuarios')