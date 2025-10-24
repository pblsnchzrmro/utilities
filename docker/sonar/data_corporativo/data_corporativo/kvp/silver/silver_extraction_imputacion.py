# Databricks notebook source
# MAGIC %md
# MAGIC # Extract

# COMMAND ----------

import requests
import os
import json
from datetime import datetime, timedelta
import calendar
import time
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS operaciones.silver_kvp")

# COMMAND ----------

# DBTITLE 1,Generar fechas
def generate_date_pairs(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    
    date_pairs = []
    current_start = start_date
    
    while current_start <= end_date:
        # Get the last day of the current month
        current_year = current_start.year
        current_month = current_start.month
        last_day_of_month = calendar.monthrange(current_year, current_month)[1]
        current_end = datetime(current_year, current_month, last_day_of_month)
        
        # Adjust the current_end to the end_date if it exceeds it
        if current_end > end_date:
            current_end = end_date
        
        date_pairs.append([current_start.strftime("%d/%m/%Y"), current_end.strftime("%d/%m/%Y")])
        
        # Move to the start of the next month
        current_start = current_end + timedelta(days=1)
    
    return date_pairs

def last_day_prev_month():
    # Get the last day of the previous month
    last_day = datetime.today().replace(day=1) - relativedelta(days=1)

    # Return it in string format
    return last_day.strftime('%d/%m/%Y')

# Dos meses previos completos
def generate_date_pairs_from_recent_months():
    today = datetime.today()
    
    # Determinar el último día del mes pasado
    last_month = today.month - 1 if today.month > 1 else 12
    last_month_year = today.year if today.month > 1 else today.year - 1
    end_date = datetime(last_month_year, last_month, calendar.monthrange(last_month_year, last_month)[1])
    
    # Determinar el primer día de dos meses atrás
    two_months_ago = today.month - 2 if today.month > 2 else 12 + (today.month - 2)
    two_months_ago_year = today.year if today.month > 2 else today.year - 1
    start_date = datetime(two_months_ago_year, two_months_ago, 1)
    
    # Generar pares de fechas
    date_pairs = []
    current_start = start_date
    
    while current_start <= end_date:
        # Obtener el último día del mes actual
        current_year = current_start.year
        current_month = current_start.month
        last_day_of_month = calendar.monthrange(current_year, current_month)[1]
        current_end = datetime(current_year, current_month, last_day_of_month)
        
        # Ajustar el current_end para no exceder el end_date
        if current_end > end_date:
            current_end = end_date
        
        date_pairs.append([current_start.strftime("%d/%m/%Y"), current_end.strftime("%d/%m/%Y")])
        
        # Mover al inicio del siguiente mes
        current_start = current_end + timedelta(days=1)
    
    return date_pairs

# Mes previo y el último día del mes actual
def generate_date_pairs_from_recent_monthsv2():
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    
    # Last completed month
    if today.month == 1:  # January
        last_month = 12
        last_month_year = today.year - 1
    else:
        last_month = today.month - 1
        last_month_year = today.year
    
    last_month_start = datetime(last_month_year, last_month, 1)
    last_month_end = datetime(today.year, today.month, 1) - timedelta(days=1)
    
    # Current month
    current_month_start = datetime(today.year, today.month, 1)
    current_month_end = yesterday
    
    date_pairs = []
    
    # Add last completed month
    date_pairs.append([last_month_start.strftime("%d/%m/%Y"), last_month_end.strftime("%d/%m/%Y")])
    
    # Add current month only if we're not on the first day of the month
    if today.day != 1:
        date_pairs.append([current_month_start.strftime("%d/%m/%Y"), current_month_end.strftime("%d/%m/%Y")])
    
    return date_pairs

# Dos meses completos, el anterior y el actual
def generate_date_pairs_from_recent_monthsv3():
    today = datetime.today()
    
    # Previous month calculation
    if today.month == 1:  # January
        previous_month = 12
        previous_month_year = today.year - 1
    else:
        previous_month = today.month - 1
        previous_month_year = today.year
    
    # Calculate first and last day of previous month
    previous_month_start = datetime(previous_month_year, previous_month, 1)
    
    # Last day of previous month (first day of current month minus one day)
    previous_month_end = datetime(today.year, today.month, 1) - timedelta(days=1)
    
    # Current month calculation
    current_month_start = datetime(today.year, today.month, 1)
    
    # Last day of current month
    if today.month == 12:
        current_month_end = datetime(today.year + 1, 1, 1) - timedelta(days=1)
    else:
        current_month_end = datetime(today.year, today.month + 1, 1) - timedelta(days=1)
    
    # Create date pairs
    date_pairs = [
        [previous_month_start.strftime("%d/%m/%Y"), previous_month_end.strftime("%d/%m/%Y")],
        [current_month_start.strftime("%d/%m/%Y"), current_month_end.strftime("%d/%m/%Y")]
    ]
    
    return date_pairs

# COMMAND ----------

# full_load = 'Y' 
full_load = 'N'

# COMMAND ----------

# DBTITLE 1,conexion + extraccion
# Conexión KVP
kvp_url="https://kvpproject.hiberus.com/usuarios/auth"
 
payload = json.dumps({
  "username": "ZZZ_DATA",
  "password": "8b84cb84e3e151abe4fccb3dfd7f67d8",
  "azure": False
})
headers = {
  'Content-Type': 'application/json'
}

if full_load == 'Y':
    desde_fecha= "01/01/2023"
    hasta_fecha= last_day_prev_month()
    date_pair_list = generate_date_pairs(desde_fecha, hasta_fecha)
if full_load == 'N':
    date_pair_list = generate_date_pairs_from_recent_monthsv3()

# Se inicializa el dataframe a vacío
schema = StructType([])
kvp_full_df = spark.createDataFrame([],schema)

response = requests.request("POST", kvp_url, headers=headers, data=payload)

if response.status_code == 200:
    formatted_res = response.json()
    token = formatted_res['token']
    header = { 'Content-Type': 'application/json', 'Authorization': 'Bearer '+ token }

    # Se realizan las llamadas a la API
    for start_date, end_date in date_pair_list:
        final_url = f"https://kvpproject.hiberus.com/imputacion/informe-detalladoSinAgrupar?desde={start_date}&hasta={end_date}"

        get_res = requests.request("GET", final_url, headers=header)

    
        if get_res.status_code == 200:
            datos_json = get_res.json()
            if isinstance(datos_json, list):
                if datos_json:
                    sample_data = datos_json[0]
                    schema = StructType([StructField(key, StringType(), True) for key in sample_data.keys()])
                    df = spark.createDataFrame(datos_json, schema)
                else:
                    df = spark.createDataFrame([], schema)
            else:
                sample_data = datos_json
                schema = StructType([StructField(key, StringType(), True) for key in sample_data.keys()])
                df = spark.createDataFrame([datos_json], schema)

            if kvp_full_df.isEmpty():
                kvp_full_df = df
            else:
                kvp_full_df = kvp_full_df.union(df)

            print("Data extraction from " + start_date + " to " + end_date + " status: 200 OK.")

        else:
            print("Error occurred while making API request due to: ")
            print(get_res.content)

        get_res.close()
        time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

# DBTITLE 1,Funcion
# def verify_latest_date(kvp_full_df, date_pair_list):
#     # Get the last end date from date_pair_list
#     last_end_date_str = date_pair_list[-1][1]
#     last_end_date = datetime.strptime(last_end_date_str, "%d/%m/%Y")
    
#     # Check if 'fecha' column exists
#     if 'fecha' not in kvp_full_df.columns:
#         print("Warning: 'fecha' column not found in the dataframe")
#         return False
    
#     # Find the maximum date in the dataframe
#     max_date_result = (
#         kvp_full_df
#         .select(F.to_date('fecha', 'yyyy-MM-dd').alias('parsed_fecha'))
#         .agg(F.max('parsed_fecha').alias('latest_date'))
#         .collect()[0]['latest_date']
#     )
    
#     # Compare dates
#     if max_date_result:
#         is_matching = max_date_result == last_end_date.date()
        
#         print(f"Date Verification:")
#         print(f"Last End Date in Pairs: {last_end_date_str}")
#         print(f"Latest Date in DataFrame: {max_date_result}")
#         print(f"Dates Match: {is_matching}")
        
#         return is_matching
    
#     print("No dates found in the dataframe")
#     return False



# COMMAND ----------

# DBTITLE 1,funcion fecha v2
def verify_latest_date(kvp_full_df, date_pair_list, tolerance_days=3, allow_earlier_if_weekend=True):
    """
    Verify that the latest date in dataframe matches (or is within tolerance) of the last end date in date_pair_list.
    
    Parameters:
    - kvp_full_df: Spark DataFrame containing the data with a 'fecha' column
    - date_pair_list: List of tuples with (start_date, end_date) in format 'dd/mm/yyyy'
    - tolerance_days: Number of days to allow for difference between dates (default: 3)
    - allow_earlier_if_weekend: If True, accept earlier dates if last date falls on a weekend (default: True)
    
    Returns:
    - Boolean indicating if the dates match within the specified tolerance
    """
    import calendar
    from datetime import datetime, timedelta
    
    # Get the last end date from date_pair_list
    last_end_date_str = date_pair_list[-1][1]
    last_end_date = datetime.strptime(last_end_date_str, "%d/%m/%Y")
    
    # Check if 'fecha' column exists
    if 'fecha' not in kvp_full_df.columns:
        print("Warning: 'fecha' column not found in the dataframe")
        return False
    
    # Find the maximum date in the dataframe
    max_date_result = (
        kvp_full_df
        .select(F.to_date('fecha', 'yyyy-MM-dd').alias('parsed_fecha'))
        .agg(F.max('parsed_fecha').alias('latest_date'))
        .collect()[0]['latest_date']
    )
    
    if not max_date_result:
        print("No dates found in the dataframe")
        return False
    
    # Calculate difference in days
    days_diff = (last_end_date.date() - max_date_result).days
    
    # Check if the last end date is a weekend
    is_weekend = last_end_date.weekday() >= 5  # 5 is Saturday, 6 is Sunday
    
    # If weekend, find the closest previous business day
    closest_business_day = last_end_date
    if is_weekend and allow_earlier_if_weekend:
        while closest_business_day.weekday() >= 5:
            closest_business_day = closest_business_day - timedelta(days=1)
    
    # Determine if the dates match or are within tolerance
    is_matching = False
    
    if is_weekend and allow_earlier_if_weekend:
        # If weekend, check if the max date is within tolerance of the closest business day
        days_to_business_day = (closest_business_day.date() - max_date_result).days
        is_matching = abs(days_to_business_day) <= tolerance_days
    else:
        # Otherwise, check if max date is within tolerance of the last end date
        is_matching = abs(days_diff) <= tolerance_days
    
    # Print verification details
    print(f"Date Verification:")
    print(f"Last End Date in Pairs: {last_end_date_str} {'(Weekend)' if is_weekend else ''}")
    if is_weekend and allow_earlier_if_weekend:
        print(f"Closest Business Day: {closest_business_day.strftime('%d/%m/%Y')}")
    print(f"Latest Date in DataFrame: {max_date_result}")
    print(f"Days Difference: {days_diff}")
    print(f"Tolerance: {tolerance_days} days")
    print(f"Dates Match Within Tolerance: {is_matching}")
    
    return is_matching

# Example usage:
# verify_latest_date(kvp_full_df, date_pair_list, tolerance_days=2)

# COMMAND ----------

# DBTITLE 1,Save + check

if kvp_full_df is not None and not kvp_full_df.isEmpty():

    dates_match = verify_latest_date(kvp_full_df, date_pair_list)
    
    if not dates_match:
        
        print("WARNING: Latest date does not match expected end date")
        # Harakiri al job 
        raise Exception("Fallo intencional, no coincide fecha")
    else:
        if full_load == 'Y':
            kvp_full_df.write.mode("overwrite").format("delta").saveAsTable("operaciones.silver_kvp.imputacion")

        if full_load == 'N':
            
            min_date_result = (
                kvp_full_df
                .select(F.to_date('fecha', 'yyyy-MM-dd').alias('parsed_fecha'))
                .agg(F.min('parsed_fecha').alias('earliest_date'))
                .collect()[0]['earliest_date']
            )
            
            min_date_str = min_date_result.strftime("%Y-%m-%d")

            spark.sql(f"""
                DELETE FROM operaciones.silver_kvp.imputacion
                WHERE fecha >= '{min_date_str}'
            """)

            kvp_full_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("operaciones.silver_kvp.imputacion")

# COMMAND ----------

# import datetime

# # Get the current date
# current_date = datetime.datetime.now()

# # Check if the current day is 10th or later
# if current_date.day >= 10:
#     # Proceed with the date verification
#     dates_match = verify_latest_date(kvp_full_df, date_pair_list)
    
#     # Optional: You can add additional logic based on the result
#     if not dates_match:
#         # Log a warning or take alternative action
#         print("WARNING: Latest date does not match expected end date")
#         # Raise an exception to fail the job
#         raise Exception("Fallo intencional, no coincide fecha")
# else:
#     print("Skipping date verification: It's before the 10th of the month")


# COMMAND ----------

# def merge_delta_data(kvp_full_df, table_path, date_pair_list=None):
#     try:
#         print("Starting the merge process...")
        
#         # Verify date if date_pair_list is provided
#         if date_pair_list:
#             print("Verifying dates...")
#             # Call the verify_latest_date function for date verification
#             dates_match = verify_latest_date(kvp_full_df, date_pair_list)

#             # If dates don't match, return False
#             if not dates_match:
#                 print("Date verification failed. Aborting merge.")
#                 return False

#         print("Creating unique record hash...")
#         # Create a unique identifier that considers idProyecto, IdUsuario, and fecha
#         kvp_full_df_with_hash = kvp_full_df.withColumn(
#             "unique_record_hash", 
#             md5(concat_ws("_", 
#                 col("idUsuarioMte"), 
#                 col("idProyecto"), 
#                 col("fecha")
#             ))
#         )
        
#         display(kvp_full_df_with_hash)  # Display the DataFrame with the unique record hash
        
#         # Check if the Delta table exists
#         schema_name = table_path.split('.')[1]
#         table_name = table_path.split('.')[-1]
#         if spark.sql(f"SHOW TABLES IN {schema_name}").filter(f"tableName = '{table_name}'").count() > 0:
#             print("Delta table exists. Performing merge operation...")
#             # Perform merge operation using SQL
#             kvp_full_df_with_hash.createOrReplaceTempView("new_data")
#             spark.sql(f"""
#                 MERGE INTO {table_path} AS existing_table
#                 USING new_data
#                 ON existing_table.IdUsuario = new_data.IdUsuario 
#                 AND existing_table.idProyecto = new_data.idProyecto 
#                 AND existing_table.fecha = new_data.fecha
#                 WHEN MATCHED THEN
#                     UPDATE SET *
#                 WHEN NOT MATCHED THEN
#                     INSERT *
#             """)
#             print("Merge operation completed successfully.")
#         else:
#             print("Delta table does not exist. Writing as new...")
#             # If table doesn't exist, write as new
#             kvp_full_df_with_hash.write.format("delta").mode("overwrite").save(table_path)
#             print("New Delta table created successfully.")
        
#         # Display the newly loaded data after merge
#         display(spark.sql(f"SELECT * FROM {table_path} WHERE unique_record_hash IN (SELECT unique_record_hash FROM new_data)"))
        
#         print("Data merged successfully into Delta table")
#         return True
    
#     except Exception as e:
#         print(f"Error merging data: {e}")
#         return False

# COMMAND ----------

# table_path = "operaciones.silver_kvp.imputacion"
# merge_delta_data(kvp_full_df, table_path, date_pair_list)

# COMMAND ----------

# %sql
# WITH date_calculation AS (
#   SELECT 
#     -- Current month end calculation
#     LAST_DAY(CURRENT_DATE()) AS expected_end_date,
    
#     -- Maximum date from the table
#     MAX(fecha) AS max_date_in_table
#   FROM operaciones.silver_kvp.imputacion
# )

# SELECT 
#   expected_end_date,
#   max_date_in_table,
#   CASE 
#     WHEN expected_end_date = max_date_in_table THEN 'Match'
#     ELSE 'No Match'
#   END AS date_match_status,
#   CURRENT_TIMESTAMP() AS check_timestamp
# FROM date_calculation


# COMMAND ----------

# DBTITLE 1,Save
# if full_load == 'Y':
#     kvp_full_df.write.mode("overwrite").format("delta").saveAsTable("operaciones.silver_kvp.imputacion")

# if full_load == 'N':
#     start_dates = [datetime.strptime(pair[0], "%d/%m/%Y") for pair in date_pair_list]
#     # Encontrar la fecha mínima
#     min_date = min(start_dates)
#     # Convertir al formato YYYY-MM-DD
#     min_date_str = min_date.strftime("%Y-%m-%d")

#     # Spark SQL para eliminar filas ya en la tabla
#     spark.sql(f"""
#         DELETE FROM operaciones.silver_kvp.imputacion
#         WHERE fecha >= '{min_date_str}'
#     """)

#     kvp_full_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("operaciones.silver_kvp.imputacion")