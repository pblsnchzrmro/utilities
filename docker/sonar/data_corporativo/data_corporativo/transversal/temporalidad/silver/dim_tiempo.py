# Databricks notebook source
# Define the file path to your CSV file
file_path = '/Volumes/transversal/silver_temporalidad/source_csv/dim_tiempo(Tiempo).csv'

# Read the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(file_path)

column_names = ["Tiempo", "Hora", "Minuto", "HoraMinuto", "TimeIndex", "TramoHora"]
df.toDF(*column_names)

# Display the DataFrame content
df.display()

# Print the schema of the DataFrame
df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import date_format

# Transform timestamp columns to HH:MM:SS

df = df.withColumn("Tiempo", date_format("Tiempo", "HH:mm:ss"))
df = df.withColumn("HoraMinuto", date_format("HoraMinuto", "HH:mm"))
df = df.withColumn("TramoHora", date_format("TramoHora", "HH:mm:ss"))

df.display()



# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").option("enableColumnMapping", "true").saveAsTable('transversal.silver_temporalidad.dim_tiempo')
