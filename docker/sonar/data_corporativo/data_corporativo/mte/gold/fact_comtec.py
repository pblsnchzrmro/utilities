# Databricks notebook source
# Databricks notebook to explode IdComunidadesTecnologicas into separate rows
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, IntegerType

# Step 1: Read the fact_usuarios table
fact_usuarios = spark.table("operaciones.silver_mte.fact_usuarios")

# Step 2: Filter out null or empty IdComunidadesTecnologicas
filtered_df = fact_usuarios.filter(
    (F.col("IdComunidadesTecnologicas").isNotNull()) & 
    (F.trim(F.col("IdComunidadesTecnologicas")) != "")
)

# Step 3: Define a UDF to split, trim, and cast the IdComunidadesTecnologicas column
def split_trim_and_cast(tech_ids_str):
    if not tech_ids_str:
        return []
    return [int(tech_id.strip()) for tech_id in tech_ids_str.split(",")]

split_udf = F.udf(split_trim_and_cast, ArrayType(IntegerType()))

# Step 4: Apply the UDF and explode the result
exploded_df = filtered_df.withColumn(
    "IdComunidadesTecnologicas_array", 
    split_udf(F.col("IdComunidadesTecnologicas"))
).select(
    F.col("id").cast(IntegerType()).alias("IdUsuario"),
    F.explode("IdComunidadesTecnologicas_array").alias("IdComTec")
)

# Add a current_timestamp column and call it FechaCarga
exploded_df_with_timestamp = exploded_df.withColumn("FechaCarga", F.current_timestamp())

# Step 5: Create or replace the new table with the timestamp column
exploded_df_with_timestamp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("operaciones.gold_mte.fact_comtec")
exploded_df_with_timestamp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("people.gold_mte.fact_comtec")