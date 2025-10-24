# Databricks notebook source
import requests
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Workspace/data_corporativo/nailted/utils/commons

# COMMAND ----------

access_token = get_token_nailted()

# COMMAND ----------

url_mensajes = 'https://app.nailted.com/api/feedback-insights'
headers = {
    'Authorization': access_token
}
parameters = {
	"startsAt": str(first_day),
	"endsAt": str(last_day)
  	# 'days': '90'
}

# COMMAND ----------

content = request_data(url_mensajes, headers, parameters)

# COMMAND ----------

print(content['data'])

# COMMAND ----------

print(content['data']['zaragoza'])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json

# Define the schema
schema = StructType([
    StructField("group", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("positive", IntegerType(), True),
    StructField("negative", IntegerType(), True)
])

# Parse the string into a dictionary
data_dict = content['data']

# Convert the dictionary into a list of dictionaries for DataFrame
data = []
for group, metrics in data_dict.items():
    for metric, values in metrics.items():
        values['positive'] = int(values['positive'])
        values['negative'] = int(values['negative'])
        data.append(values)

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.display()

# COMMAND ----------

from pyspark.sql.functions import col, lit

# Add 'year' and 'month' columns
df = df.withColumn('year', lit(year))
df = df.withColumn('month', lit(month))

# Rearrange columns order to place 'year' and 'month' at the start
new_columns_order = ['year', 'month'] + [col for col in df.columns if col not in ['year', 'month']]

df = df.select([col(column) for column in new_columns_order])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Add current date column in datetime format
df = df.withColumn("FECHA_CARGA", current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

target_table = 'people.bronze_nailted.mensajes'

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

# COMMAND ----------

# df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").option("enableColumnMapping", "true").saveAsTable('people.bronze_nailted.metricas')