# Databricks notebook source
import requests
import json
from pyspark.sql.functions import col, concat_ws, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, DoubleType, IntegerType, BooleanType

# COMMAND ----------

# MAGIC %run /Workspace/data_corporativo/mte/utils/common

# COMMAND ----------


url = "https://mte-produccion.azure-api.net/data/v1/ComunidadesOcio/Items"
token_formatted = API_MTE_token()

data = request_data(url, token_formatted)
print(data)

# COMMAND ----------

schema = StructType([
    StructField("Nombre", StringType(), True),
    StructField("NivelMadurez", StringType(), True),
    StructField("Responsable", ArrayType(StructType([
        StructField("Id", IntegerType(), True),
        StructField("Value", StringType(), True),
        StructField("Email", StringType(), True)
    ])), True),
    StructField("ID", IntegerType(), True),
])

mte_df = spark.createDataFrame(data, schema=schema)
# mte_df = spark.createDataFrame(data)
mte_df.display()

# COMMAND ----------

mte_formatted = mte_df.select( \
    col("Nombre"), \
    col("NivelMadurez"), \
    col("ID"), \
    concat_ws(", ", expr("TRANSFORM(Responsable, x -> x.Value)")).alias("Responsable"), \
    concat_ws(", ", expr("TRANSFORM(Responsable, x -> x.Email)")).alias("ResponsableEmail"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, to_date

# Add current date column in 'dd-MM-yyyy' format
mte_formatted = mte_formatted.withColumn("FECHA_CARGA",current_timestamp())

# COMMAND ----------

mte_formatted.display()

# COMMAND ----------

mte_formatted.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.bronze_mte.ComunidadesOcio')
mte_formatted.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.bronze_mte.ComunidadesOcio')