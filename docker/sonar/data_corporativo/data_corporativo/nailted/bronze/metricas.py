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

url_metrics = 'https://app.nailted.com/api/metric'
headers = {
    'Authorization': access_token
}
parameters = {
	"startsAt": str(first_day),
	"endsAt": str(last_day)
  	# 'days': '90'
}

# COMMAND ----------

content = request_data(url_metrics, headers, parameters)

# COMMAND ----------

# Schema definition
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("group", StringType(), True),
    StructField("createdAt", TimestampType(), True),
    StructField("participants", IntegerType(), True),
    StructField("metrics", StringType(), True),
    StructField("responders", IntegerType(), True),
    StructField("periodStartsAt", StringType(), True),
    StructField("periodEndsAt", TimestampType(), True),
    StructField("enps", IntegerType(), True),
    StructField("enps_detractors", IntegerType(), True),
    StructField("enps_passives", IntegerType(), True),
    StructField("enps_promoters", IntegerType(), True),
    StructField("alignment", FloatType(), True),
    StructField("feedback-frequency", FloatType(), True),
    StructField("satisfaction", FloatType(), True),
    StructField("happiness-at-work", FloatType(), True),
    StructField("peers", FloatType(), True),
    StructField("ambassadorship", FloatType(), True),
    StructField("praise", FloatType(), True),
    StructField("feedback", FloatType(), True),
    StructField("satisfaction-compensation", FloatType(), True),
    StructField("praise-quality", FloatType(), True),
    StructField("management-trust", FloatType(), True),
    StructField("work-life-balance", FloatType(), True),
    StructField("psafety", FloatType(), True),
    StructField("engagement", FloatType(), True),
    StructField("growth", FloatType(), True),
    StructField("motivation", FloatType(), True),
    StructField("suggestions-for-the-organization", FloatType(), True),
    StructField("wellness", FloatType(), True),
    StructField("happiness", FloatType(), True),
    StructField("peers-collaboration", FloatType(), True),
    StructField("management", FloatType(), True),
    StructField("criteria", StructType([
        StructField("operator", StringType(), True),
        StructField("conditions", ArrayType(StructType([
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("period", StringType(), True),
            StructField("condition", StringType(), True)
        ])), True)
    ]), True),
    StructField("category",StructType([
    StructField("id", IntegerType(), True),
    StructField("org", IntegerType(), True),
    StructField("name", StructType([
        StructField("en", StringType(), True),
        StructField("es", StringType(), True)
    ]), True),
    StructField("type", StringType(), True),
    StructField("includeAnalytics", StringType(), True),
    StructField("createdBy", IntegerType(), True),
    StructField("defaultAddedAt", StringType(), True)
]), True),
    StructField("calculationInProgress", StringType(), True),
    StructField("calculationStartedAt", TimestampType(), True),
    StructField("access", StringType(), True),
    StructField("groupCreatedBy", IntegerType(), True),
    StructField("groupParentId", StringType(), True),
    StructField("conflicts", StringType(), True),
    StructField("builtIn", StringType(), True),
    StructField("conflictGroups", StringType(), True),
    StructField("creator", StructType([
    StructField("id", IntegerType(), True),
    StructField("fullName", StringType(), True),
    StructField("email", StringType(), True)])
    , True)
])

# COMMAND ----------

print([content['data']])

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import to_timestamp, date_format


data = content['data']

# Convert datetime fields to the correct format if necessary
for e in data:
    for field in schema.fields:
        if field.name in e and isinstance(e[field.name], str):
            if field.dataType == TimestampType():
                e[field.name] = datetime.strptime(e[field.name], '%Y-%m-%dT%H:%M:%S.%fZ')
            elif field.dataType == IntegerType():
                e[field.name] = int(e[field.name])



df = spark.createDataFrame(data, schema=schema)

df = df.withColumn("periodStartsAt",
                   to_timestamp("periodStartsAt", "MM/dd/yyyy"))




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

display(df)

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

target_table = 'people.bronze_nailted.metricas'

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

# COMMAND ----------

# df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").option("enableColumnMapping", "true").saveAsTable('people.bronze_nailted.metricas')