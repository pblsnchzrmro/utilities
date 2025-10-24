# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr

# COMMAND ----------

# Read bronze table

df = spark.read.table("people.bronze_nailted.mensajes")

# COMMAND ----------

display(df)

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

target_table = 'people.silver_nailted.mensajes'

# Check if the target table exists
if not spark.catalog.tableExists(target_table):
    # Create the target table with the schema of the source DataFrame
    df.write.format("delta").saveAsTable(target_table)

# Load the target table into a DeltaTable
target_table = DeltaTable.forName(spark, target_table)

# Perform the merge operation
(target_table.alias('target')
 .merge(df.alias('source'), 'source.year = target.year AND source.month = target.month AND source.group = target.group AND source.metric = target.metric')
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())