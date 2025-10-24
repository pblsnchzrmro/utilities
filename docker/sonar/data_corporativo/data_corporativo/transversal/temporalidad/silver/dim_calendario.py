# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

# Define the file path to your CSV file
file_path = '/Volumes/transversal/silver_temporalidad/source_csv/dim_calendario(Calendario).csv'

# Schema
schema = StructType([
    StructField("Fecha", DateType(), True),
    StructField("FechaCompleta", StringType(), True),
    StructField("Año", IntegerType(), True),
    StructField("Mes", IntegerType(), True),
    StructField("NombreMes", StringType(), True),
    StructField("MesCorto", StringType(), True),
    StructField("Dia", IntegerType(), True),
    StructField("DiaSemana", IntegerType(), True),
    StructField("NombreDia", StringType(), True),
    StructField("DiaCorto", StringType(), True),
    StructField("DiaMesNum", StringType(), True),
    StructField("Dia/Numdia/Mes", StringType(), True),
    StructField("DiaMes", StringType(), True),
    StructField("AñoMes", StringType(), True),
    StructField("AñoMesCorto", StringType(), True),
    StructField("OrdenAñoMes", IntegerType(), True),
    StructField("Trimestre", StringType(), True),
    StructField("AñoTrimestre", StringType(), True),
    StructField("OrdenAñoTrimestre", IntegerType(), True),
    StructField("Semestre", StringType(), True),
    StructField("AñoSemestre", StringType(), True),
    StructField("OrdenAñoSemestre", IntegerType(), True),
    StructField("PrimerDiaSemana", DateType(), True),
    StructField("UltimoDiaSemana", DateType(), True),
    StructField("PrimerDiaMes", DateType(), True),
    StructField("UltimoDiaMes", DateType(), True),
    StructField("PrimerDiaTrimestre", DateType(), True),
    StructField("UltimoDiaTrimestre", DateType(), True),
    StructField("PrimerDiaAño", DateType(), True),
    StructField("UltimoDiaAño", DateType(), True),
    StructField("Semana", StringType(), True),
    StructField("OrdenSemana", IntegerType(), True),
    StructField("AñoSemana", StringType(), True),
    StructField("OrdenAñoSemana", IntegerType(), True),
    StructField("Laboral/FinSemana", StringType(), True),
    StructField("CuentaLaborable", IntegerType(), True),
    StructField("CuentaFinDeSemana", IntegerType(), True),
    StructField("Estacion", StringType(), True),
    StructField("DiasInicioAño", IntegerType(), True),
    StructField("DiasFinAño", IntegerType(), True),
    StructField("DesvioDias", IntegerType(), True),
    StructField("DesvioDiasRelativo", StringType(), True),
    StructField("Rolling30Dias", StringType(), True),
    StructField("DesvioSemana", IntegerType(), True),
    StructField("DesvioSemanaRelativo", StringType(), True),
    StructField("DesvioMes", IntegerType(), True),
    StructField("DesvioMesRelativo", StringType(), True),
    StructField("DesvioTrimestre", IntegerType(), True),
    StructField("DesvioAño", IntegerType(), True),
    StructField("Periodo", StringType(), True),
    StructField("IdFecha", IntegerType(), True),
    StructField("CorteHoy", StringType(), True)
])

# Read the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("dateFormat", "dd/MM/yyyy") \
    .option("encoding", "latin1") \
    .schema(schema) \
    .load(file_path)



# Display the DataFrame content
df.display()

# Print the schema of the DataFrame
df.printSchema()


# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").option("enableColumnMapping", "true").saveAsTable('transversal.silver_temporalidad.dim_calendario')
