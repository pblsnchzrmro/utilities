# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.silver_mte.TarifaCoste")

# Show the first few rows of the dataframe
df.display()

# COMMAND ----------

df = df.select(df["ID"], df["TarifaCoste"].alias("Coste")) 
df = df.withColumnRenamed('ID', 'IdUsuario').withColumn('IdUsuario', F.col('IdUsuario').cast('int'))

df = df.orderBy("IdUsuario")

df.display()

# COMMAND ----------

from delta.tables import DeltaTable

# Save to GOLD catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.gold_mte.fact_costes')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.gold_mte.fact_costes')

# COMMAND ----------

from datetime import datetime, timedelta
from delta.tables import DeltaTable

# Check if today is the last day of the current month
today = datetime.today()
first_day_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
is_last_day_of_month = today == first_day_next_month - timedelta(days=1)

if is_last_day_of_month:

    # MERGE HIST TABLE

    target_table = 'operaciones.gold_mte.fact_costes_hist'

    # Drop column Fecha_carga
    # df = df.drop('FECHA_CARGA')

    # Check if the target table exists
    if not spark.catalog.tableExists(target_table):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table)

    # Load the target table into a DeltaTable
    target_table = DeltaTable.forName(spark, target_table)

    # Drop column Fecha_carga
    # df = df.drop('FECHA_CARGA')

    # Perform the merge operation
    (target_table.alias('target')
    .merge(df.alias('source'), 'source.IdUsuario = target.IdUsuario')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

    # MERGE HIST TABLE for people catalog

    target_table_people = 'people.gold_mte.fact_costes_hist'

    # Check if the target table exists
    if not spark.catalog.tableExists(target_table_people):
        # Create the target table with the schema of the source DataFrame
        df.write.format("delta").saveAsTable(target_table_people)

    # Load the target table into a DeltaTable
    target_table_people = DeltaTable.forName(spark, target_table_people)

    # Perform the merge operation
    (target_table_people.alias('target')
    .merge(df.alias('source'), 'source.IdUsuario = target.IdUsuario')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp, col, sha2
# from delta.tables import DeltaTable
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# def enhanced_historical_data_tracking(spark: SparkSession, df, 
#                                       current_table: str, 
#                                       historical_table: str, 
#                                       primary_key: str, 
#                                       track_columns: list):
#     """
#     Enhanced historical data tracking with advanced features
    
#     Args:
#     - spark: SparkSession
#     - df: DataFrame to be processed
#     - current_table: Name of the current state table
#     - historical_table: Name of the historical tracking table
#     - primary_key: Column to use as primary key for merging
#     - track_columns: List of columns to track changes
#     """
#     # Add metadata columns
#     df_with_metadata = (df
#         .withColumn('LOAD_TIMESTAMP', current_timestamp())
#         .withColumn('RECORD_HASH', 
#             sha2(concat_ws('||', *[col(c) for c in track_columns]), 256)
#         )
#     )
    
#     # Drop FECHA_CARGA if it exists
#     if 'FECHA_CARGA' in df_with_metadata.columns:
#         df_with_metadata = df_with_metadata.drop('FECHA_CARGA')
    
#     # Overwrite current table
#     df_with_metadata.write.mode("overwrite").format("delta") \
#         .option("overwriteSchema", "true") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(current_table)
    
#     # Check and prepare historical table
#     if not spark.catalog.tableExists(historical_table):
#         # Create historical table with enhanced schema
#         historical_schema = StructType([
#             *df_with_metadata.schema.fields,
#             StructField("VALID_FROM", TimestampType(), True),
#             StructField("VALID_TO", TimestampType(), True),
#             StructField("IS_CURRENT", StringType(), True),
#             StructField("VERSION_NUMBER", LongType(), True)
#         ])
        
#         # Create empty table with the enhanced schema
#         spark.createDataFrame(
#             spark.sparkContext.emptyRDD(), 
#             historical_schema
#         ).write.format("delta").saveAsTable(historical_table)
    
#     # Load the historical Delta table
#     historical_delta_table = DeltaTable.forName(spark, historical_table)
    
#     # Prepare merge conditions and actions
#     merge_condition = f"source.{primary_key} = target.{primary_key} AND target.IS_CURRENT = 'Y'"
    
#     # Merge operation with advanced tracking
#     (historical_delta_table.alias('target')
#      .merge(
#          df_with_metadata.alias('source'), 
#          merge_condition
#      )
#      .whenMatchedUpdate(
#          condition = f"source.RECORD_HASH != target.RECORD_HASH",
#          set = {
#              "VALID_TO": current_timestamp(),
#              "IS_CURRENT": lit("N")
#          }
#      )
#      .whenNotMatched().insertAll(
#          values = {
#              **{c: f"source.{c}" for c in df_with_metadata.columns},
#              "VALID_FROM": current_timestamp(),
#              "VALID_TO": None,
#              "IS_CURRENT": lit("Y"),
#              "VERSION_NUMBER": "target.VERSION_NUMBER + 1"
#          }
#      )
#      .execute()
#     )
    
#     return df_with_metadata

# # Example usage
# def main(spark):
#     # Sample DataFrame
#     sample_data = spark.createDataFrame([
#         (1, "John", "Doe", 1000),
#         (2, "Jane", "Smith", 2000)
#     ], ["IdUsuario", "Nombre", "Apellido", "Salario"])
    
#     # Call the enhanced tracking function
#     enhanced_historical_data_tracking(
#         spark, 
#         sample_data, 
#         'operaciones.gold_mte.fact_costes',
#         'operacional.gold_mte.fact_costes_hist',
#         primary_key='IdUsuario',
#         track_columns=['Nombre', 'Apellido', 'Salario']
#     )

# # Additional helper functions can be added as needed