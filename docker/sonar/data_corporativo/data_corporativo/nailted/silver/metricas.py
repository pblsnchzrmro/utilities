# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr

# COMMAND ----------

# Read bronze table

df = spark.read.table("people.bronze_nailted.metricas")

# COMMAND ----------

display(df)

# COMMAND ----------

metricas = df.select('metrics')
first_row = metricas.collect()[1]
metrics_value = first_row['metrics']
print(metrics_value)

# COMMAND ----------

import json

metrics_value = metrics_value.replace("'", '"')

metrics_value = metrics_value.replace("'", '"').replace("None", "null")

try:
    # Parse the JSON string
    metrics_dict = json.loads(metrics_value)
    
    # Extract the 'enps' object
    enps = metrics_dict.get('enps', None)
    
    # Display the 'enps' object
    print(enps)
except json.JSONDecodeError as e:
    print(f"JSONDecodeError: {e}")
    print(f"Problematic JSON string: {metrics_value}")

# COMMAND ----------

from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, MapType, StringType, IntegerType, FloatType

# Define the schema for the nested JSON structure
nested_schema = StructType([
    StructField("participants", IntegerType(), True),
    StructField("responses", IntegerType(), True),
    StructField("responders", IntegerType(), True),
    StructField("precision", FloatType(), True),
    StructField("promoters", IntegerType(), True),
    StructField("passives", IntegerType(), True),
    StructField("detractors", IntegerType(), True),
    StructField("npsed", IntegerType(), True),
    StructField("distribution", StringType(), True),
    StructField("sentimentNeutral", StringType(), True),
    StructField("sentimentNegative", StringType(), True),
    StructField("sentimentPositive", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("ratingByIndex", FloatType(), True)
])

# Create a schema for the top-level keys (level one) as a MapType
top_level_schema = MapType(StringType(), nested_schema)

# Parse and extract the JSON data
def extract_keys(df):
    # Filter out null or empty rows
    df = df.filter(col("metrics").isNotNull() & (col("metrics") != ""))

    df = df.withColumn("metrics", regexp_replace(col("metrics"), "'", '"'))
    df = df.withColumn("metrics", regexp_replace(col("metrics"), "None", "null"))

    # Parse the `metrics` column
    df = df.withColumn("metrics", from_json(col("metrics"), top_level_schema))

    # Extract 'enps' and 'engagement' keys if they exist
    df = df.withColumn("enps_full", col("metrics.enps"))
    df = df.withColumn("engagement_full", col("metrics.engagement"))

    return df

# Example Usage
# Assuming you have a DataFrame named `df` and a column `metrics`
df = extract_keys(df)

# Show the results
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Assuming `processed_df` is the DataFrame with extracted columns
df = df.withColumn(
    "enps_participation_rate", col("enps_full.responders") / col("enps_full.participants")
).withColumn(
    "enps_detractores_rate", col("enps_full.detractors") / col("enps_full.responses")
).withColumn(
    "enps_promoters_rate", col("enps_full.promoters") / col("enps_full.responses")
).withColumn(
    "enps_passives_rate", col("enps_full.passives") / col("enps_full.responses")
).withColumn(
    "engagement_participation_rate", col("engagement_full.responders") / col("engagement_full.participants")
)

# Show the resulting DataFrame
display(df)

# COMMAND ----------

# Select the required fields and alias them
df = df.select(
    "*",

    F.col("category")['id'].alias("categoryId"),
    F.col("category")['org'].alias("categoryOrg"),
    F.col("category")['name']['es'].alias("categoryNameES"),
    F.col("category")['type'].alias("categoryType"),


    F.col("creator")['id'].alias("creatorId"),
    F.col("creator")['fullName'].alias("creatorName"),
    F.col("creator")['email'].alias("creatorEmail")
).drop("category").drop("creator")

df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# MERGE TABLE

target_table = 'people.silver_nailted.metricas'

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