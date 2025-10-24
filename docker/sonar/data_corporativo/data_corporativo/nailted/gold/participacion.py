# Databricks notebook source
# Read silver table

df = spark.read.table("people.silver_nailted.participacion")

# COMMAND ----------

display(df)

# COMMAND ----------

# RENAME COLUMNS AND SELECT

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

target_table = 'people.gold_nailted.participacion'

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