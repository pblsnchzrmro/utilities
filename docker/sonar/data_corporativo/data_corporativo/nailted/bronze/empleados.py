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

# url_part = 'https://app.nailted.com/api/team?search=gdasi@hiberus.com'
# headers = {
#     'Authorization': access_token
# }
# parameters = {}

# COMMAND ----------

# content = request_data(url_part, headers, parameters)

# COMMAND ----------

print(content)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, TimestampType, MapType
from datetime import datetime
from pyspark.sql.functions import current_timestamp

# Define schema
manager_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("disabled", BooleanType(), True),
    StructField("oneOnOneInvitedAt", StringType(), True),
    StructField("picture", StringType(), True),
    StructField("preferredTransport", StringType(), True),
    StructField("language", StringType(), True),
])

config_settings_schema = MapType(StringType(), BooleanType())

data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("email", StringType(), True),
    StructField("manager", manager_schema, True),
    StructField("disabled", BooleanType(), True),
    StructField("surveys", BooleanType(), True),
    StructField("picture", StringType(), True),
    StructField("groups", ArrayType(StringType()), True),
    StructField("canDeliverEmail", BooleanType(), True),
    StructField("hasSlackToken", BooleanType(), True),
    StructField("skipManager", BooleanType(), True),
    StructField("rating", StringType(), True),
    StructField("ratedAt", StringType(), True),
    StructField("groupAsCompany", StringType(), True),
    StructField("disabledAt", StringType(), True),
    StructField("status", StringType(), True),
    StructField("disabledReason", StringType(), True),
    StructField("configSettings", config_settings_schema, True),
    StructField("endingDate", StringType(), True),
    StructField("createdAt", TimestampType(), True),
    StructField("updatedAt", TimestampType(), True),
    StructField("preferredTransport", StringType(), True),
    StructField("language", StringType(), True),
    StructField("user", StringType(), True),
    StructField("isNew", BooleanType(), True),
])

def create_row_df(content):
    # Convert string dates to datetime objects
    for record in content['data']:
        if 'createdAt' in record and record['createdAt']:
            record['createdAt'] = datetime.fromisoformat(record['createdAt'].replace('Z', '+00:00'))
        if 'updatedAt' in record and record['updatedAt']:
            record['updatedAt'] = datetime.fromisoformat(record['updatedAt'].replace('Z', '+00:00'))

    data = content['data']
    # Create DataFrame
    df = spark.createDataFrame(data, schema=data_schema)

    # Add FECHA_CARGA
    df = df.withColumn("FECHA_CARGA", current_timestamp())

    # Show DataFrame
    # display(df)
    return df

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

def insert_row_df(df):

    target_table = 'people.bronze_nailted.empleados'

    # Check if the target table exists
    if not spark.catalog.tableExists(target_table):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table)

    # Load the target table into a DeltaTable
    target_table = DeltaTable.forName(spark, target_table)

    # Perform the merge operation
    (target_table.alias('target')
    .merge(df.alias('source'), 'source.id = target.id')
    .whenNotMatchedInsertAll()
    .execute())

# COMMAND ----------

def process_email(email):
    url_part = f'https://app.nailted.com/api/team?search={email}'
    headers = {
        'Authorization': access_token
    }
    parameters = {}
    content = request_data(url_part, headers, parameters)
    df = create_row_df(content)
    # display(df)
    insert_row_df(df)


# COMMAND ----------

process_email('gdasi@hiberus.com')

# COMMAND ----------

final_table = 'people.bronze_nailted.empleados'
df = spark.table(final_table)
display(df)

# COMMAND ----------

df = spark.table('operaciones.gold_mte.dim_usuario')

# Collect the data and apply the function to each row
emails = df.select("EmailCorporativo").collect()
for row in emails:
    process_email(row["EmailCorporativo"])