# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr

# COMMAND ----------

# Read bronze table

df = spark.read.table("people.bronze_nailted.participacion")

# COMMAND ----------

display(df)

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

target_table = 'people.silver_nailted.participacion'

# Check if the target table exists
if not spark.catalog.tableExists(target_table):
    # Create the target table with the schema of the source DataFrame
    df.write.format("delta").saveAsTable(target_table)

# Load the target table into a DeltaTable
target_table = DeltaTable.forName(spark, target_table)

# Perform the merge operation
(target_table.alias('target')
 .merge(df.alias('source'), 'source.year = target.year AND source.month = target.month AND source.id = target.id')
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())