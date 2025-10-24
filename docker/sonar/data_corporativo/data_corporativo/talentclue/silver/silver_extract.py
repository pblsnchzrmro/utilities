# Databricks notebook source
import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from functools import reduce
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.functions import pandas_udf
import time
import pandas as pd
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validando cual es el user-agent de databricks

# COMMAND ----------

# response = requests.get("https://httpbin.org/user-agent")
# print(response.json())

# COMMAND ----------

bearer_token = 'Bearer c3VwcG9ydCtoaWJlcnVzQVBJQHRhbGVudGNsdWUuY29tOnhYS05XSENIVng1M21kb0E='   # Llevar a secret manager
url = "https://api.talentclue.com/api-v2/jobs"
headers = {
    "Accept": "application/json",
    "Authorization": bearer_token,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# COMMAND ----------

df_jobs_list = []
schema_jobs = StructType([
    StructField("id", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("companyName", StringType(), True),
    StructField("title", StringType(), True),
    StructField("created", StringType(), True),
    StructField("status", StringType(), True),
    StructField("location", StringType(), True),
    StructField("folders", StructType([
        StructField("applicants", StructType([
            StructField("number", StringType(), True)
        ]), True)
    ]), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Llamada al endpoint "jobs"

# COMMAND ----------

# page = 1
# while True:
#     response = requests.get(f"{url}?page={page}", headers=headers)
    
#     if response.status_code != 200:
#         print(f"Error {response.status_code}: {response.text}")  # Manejo de errores
#         break

#     records = response.json().get("jobs", [])
#     if not records:
#         break

#     df = spark.createDataFrame(records, schema=schema_jobs)
#     df_jobs_list.append(df)
#     page += 1

# # Unir todos los DataFrames de la lista en uno solo
# jobs = reduce(DataFrame.union, df_jobs_list)
# jobs.display()

# COMMAND ----------

# Initialize variables
page = 1
all_records = []
total_records = 0
max_retries = 3
base_wait_time = 2  # Reduced wait time for faster execution

print(f"Starting data retrieval with optimized approach")

# Main loop to fetch all pages
while True:
    try:
        print(f"Fetching page {page}...")
        response = requests.get(f"{url}?page={page}", headers=headers, timeout=30)
        
        # Simple retry logic for rate limiting
        retries = 0
        while response.status_code == 429 and retries < max_retries:
            retries += 1
            wait_time = base_wait_time * (2 ** retries)  # Exponential backoff
            print(f"Rate limited (429). Waiting {wait_time} seconds before retry {retries}/{max_retries}")
            time.sleep(wait_time)
            response = requests.get(f"{url}?page={page}", headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text[:200]}")
            break

        # Parse the response
        records = response.json().get("jobs", [])
        if not records:
            print(f"No more records found. Completed with {total_records} total records across {page-1} pages.")
            break
            
        # Collect records in memory (as dictionaries)
        all_records.extend(records)
        
        # Update counters
        records_count = len(records)
        total_records += records_count
        print(f"Retrieved {records_count} records from page {page}. Total so far: {total_records}")
        
        # Increment page
        page += 1
        
    except Exception as e:
        print(f"Error processing page {page}: {str(e)}")
        break

# Convert all collected records to a single DataFrame at once
if all_records:
    print(f"Converting {len(all_records)} records to DataFrame...")
    
    # Option 1: Create DataFrame directly from all records
    # This avoids the union operation that's causing issues
    jobs = spark.createDataFrame(all_records, schema=schema_jobs)
    
    # Option 2: If Option 1 has memory issues, use pandas as intermediary
    # pd_df = pd.DataFrame(all_records)
    # jobs = spark.createDataFrame(pd_df, schema=schema_jobs)
    
    print(f"Successfully created DataFrame. Total rows: {jobs.count()}")
    jobs.display()
else:
    print("No data was retrieved.")

# COMMAND ----------

jobs = jobs.filter(col("companyName") == "Hiberus")

# COMMAND ----------

jobs_formatted = jobs.select( \
    col("id").alias("IDPosicion"), \
    col("title").alias("TituloPosicion"), \
    col("created").alias("FechaCreacion"), \
    col("status").alias("EstadoPosicion"), \
    col("location").alias("Lugar"))
jobs_formatted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Llamada al endpoint de applicants

# COMMAND ----------

jobs_list = [row['IDPosicion'] for row in jobs_formatted.select('IDPosicion').collect()]

# COMMAND ----------

schema_applicants = StructType([
    StructField("id", StringType(), True),
    StructField("hiringStatus", StringType(), True),
    StructField("candidate", StructType([
        StructField("id", StringType(), True),
        StructField("contact", StructType([
            StructField("email", StringType(), True)
        ]), True),
        StructField("personalData", StructType([
            StructField("name", StructType([ # mal
                StructField("name", StringType(), True),
                StructField("surname", StringType(), True),
                StructField("lastSurname", StringType(), True)
            ]), True), 
            StructField("birth", StringType(), True), # mal
        ]), True),
        StructField("residence", StructType([
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
        ]), True),
        StructField("education", StructType([
            StructField("academicDegree", StringType(), True)
        ]), True),
        StructField("labour", StructType([
            StructField("headline", StringType(), True)
        ]), True),
    ]), True)
])

# COMMAND ----------

# df_applicants_list = []
# for job in jobs_list:
#     print(job)
    
#     url_applicants = f"https://api.talentclue.com/api/jobs/{job}/applicants"

#     page = 1
#     while True:
#         response_applicants = requests.get(f"{url_applicants}?page={page}&per_page=50", headers=headers)
        
#         if response.status_code != 200:
#             print(f"Error {response_applicants.status_code}: {response_applicants.text}")  # Manejo de errores
#             break

#         records_applicants = response_applicants.json().get("data", [])
#         if not records_applicants:
#             break

#         df = spark.createDataFrame(records_applicants, schema=schema_applicants) \
#              .withColumn("job_id", lit(job))
#         df_applicants_list.append(df)
#         page += 1

# # Unir todos los DataFrames de la lista en uno solo
# applicants = reduce(DataFrame.union, df_applicants_list)
# # applicants.display()

# COMMAND ----------

# Initialize a list to collect all applicant records
all_applicants = []

# Process each job
for job in jobs_list:
    print(f"Processing job: {job}")
    
    url_applicants = f"https://api.talentclue.com/api/jobs/{job}/applicants"
    job_applicants = []  # Collect applicants for this specific job
    
    page = 1
    while True:
        try:
            print(f"Fetching page {page} for job {job}...")
            response_applicants = requests.get(
                f"{url_applicants}?page={page}&per_page=50", 
                headers=headers
                # timeout=30
            )
            
            # Check response status
            if response_applicants.status_code != 200:
                print(f"Error {response_applicants.status_code}: {response_applicants.text[:200]}")
                break
            
            # Process records
            records_applicants = response_applicants.json().get("data", [])
            if not records_applicants:
                print(f"No more applicants for job {job}")
                break
            
            # Add job_id to each record
            for record in records_applicants:
                record['job_id'] = job
            
            # Add to our collection
            job_applicants.extend(records_applicants)
            print(f"Retrieved {len(records_applicants)} applicants on page {page} for job {job}")
            
            # Next page
            page += 1
            
        except Exception as e:
            print(f"Error processing job {job}, page {page}: {str(e)}")
            break
    
    print(f"Total applicants collected for job {job}: {len(job_applicants)}")
    all_applicants.extend(job_applicants)

# Create a single DataFrame from all records at once
if all_applicants:
    print(f"Creating DataFrame from {len(all_applicants)} total applicant records...")
    applicants = spark.createDataFrame(all_applicants, schema=schema_applicants)
    print(f"Successfully created DataFrame. Total rows: {applicants.count()}")
    # applicants.display()  # Uncomment if you want to display the results
else:
    print("No applicant data was retrieved.")
    applicants = None

# COMMAND ----------

applicants.display()

# COMMAND ----------

applicants_formatted = applicants.select( \
    col("hiringStatus").alias("EstadoContratacion"), \
    col("id").alias("IDPosicion"), \
    col("candidate")["ID"].alias("IDCandidato"), \
    col("candidate")["contact"]["email"].alias("Email"), \
    col("candidate")["personalData"]["name"]["name"].alias("Nombre"), \
    col("candidate")["personalData"]["name"]["surname"].alias("PrimerApellido"), \
    col("candidate")["personalData"]["birth"].alias("FechaNacimiento"), \
    col("candidate")["residence"]["country"].alias("Pais"), \
    col("candidate")["residence"]["city"].alias("Ciudad"), \
    col("candidate")["education"]["academicDegree"].alias("NivelEstudios"), \
    col("candidate")["labour"]["headline"].alias("TituloProfesion"))
applicants_formatted.display()

# COMMAND ----------

# Eliminar duplicados por el campo "Email"
applicants_formatted = applicants_formatted.dropDuplicates(['Email'])
applicants_formatted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funciones y llamadas al endpoint CV Candidates

# COMMAND ----------

# Definir la función con pandas_udf
@pandas_udf(StringType())
def cv_candidate_pandas(candidate_ids: pd.Series) -> pd.Series:
    results = []
    
    for candidate_id in candidate_ids:
        if pd.isna(candidate_id):  # Manejar valores nulos
            results.append("")
            continue

        url_candidate = f"https://api.talentclue.com/api-v2/candidates/{candidate_id}/cv?includeFileData=true"

        try:
            response_candidate = requests.get(url=url_candidate, headers=headers)
            
            if response_candidate.status_code == 200:
                data_candidate = response_candidate.json()
                results.append(data_candidate.get('data', ''))
            else:
                results.append("")  # Si hay error, devuelve vacío
        except Exception as e:
            results.append("")  # Si hay error en la solicitud, devuelve vacío
        
        time.sleep(0.1)  # Opcional: evita sobrecargar la API con demasiadas solicitudes

    return pd.Series(results)


# COMMAND ----------

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Define a function to fetch a batch of CVs (for better efficiency)
def fetch_cv_batch(candidate_ids_batch):
    results = {}
    for candidate_id in candidate_ids_batch:
        if pd.isna(candidate_id):
            results[candidate_id] = ""
            continue
            
        url_candidate = f"https://api.talentclue.com/api-v2/candidates/{candidate_id}/cv?includeFileData=true"
        
        try:
            response_candidate = requests.get(url=url_candidate, headers=headers)
            
            if response_candidate.status_code == 200:
                data_candidate = response_candidate.json()
                results[candidate_id] = data_candidate.get('data', '')
            else:
                logger.warning(f"Failed to fetch CV for candidate {candidate_id}. Status code: {response_candidate.status_code}")
                results[candidate_id] = ""
        except Exception as e:
            logger.error(f"Exception while fetching CV for candidate {candidate_id}: {str(e)}")
            results[candidate_id] = ""
        
        time.sleep(0.1)  # Avoid overloading the API
        
    return results

# Define the Pandas UDF
@pandas_udf(StringType())
def cv_candidate_pandas(candidate_ids: pd.Series) -> pd.Series:
    # Create a map of results to ensure order is preserved
    cv_map = fetch_cv_batch(candidate_ids.dropna().unique())
    
    # Map back to original Series to maintain order and handle nulls
    results = candidate_ids.map(lambda x: cv_map.get(x, "") if not pd.isna(x) else "")
    return results


# COMMAND ----------

# Aplicar la función al DataFrame
applicants_formatted = applicants_formatted.withColumn("CV", cv_candidate_pandas(applicants_formatted["IDCandidato"]))

# Mostrar resultados
applicants_formatted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export de dataframe

# COMMAND ----------

jobs_formatted.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("people.silver_talentclue.jobs")

# COMMAND ----------

applicants_formatted.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("people.silver_talentclue.applicants")