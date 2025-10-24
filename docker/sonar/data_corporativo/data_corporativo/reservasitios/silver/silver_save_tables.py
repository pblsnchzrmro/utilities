# Databricks notebook source
# DBTITLE 1,Appointments
appointments_df = spark.read.format("delta").load("/dbfs/tmp/temp_appointments")
appointments_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.appointments')

# COMMAND ----------

# DBTITLE 1,Brand
brand_df = spark.read.format("delta").load("/dbfs/tmp/temp_brand")
brand_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.brand')

# COMMAND ----------

# DBTITLE 1,Users
users_df = spark.read.format("delta").load("/dbfs/tmp/temp_users")
users_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.users')

# COMMAND ----------

# DBTITLE 1,Departments
departments_df = spark.read.format("delta").load("/dbfs/tmp/temp_departments")
departments_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.departments')

# COMMAND ----------

# DBTITLE 1,DepartmentTranslation
departmenttranslation_df = spark.read.format("delta").load("/dbfs/tmp/temp_departmenttranslation")
departmenttranslation_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.departmenttranslation')

# COMMAND ----------

# DBTITLE 1,Languages
languages_df = spark.read.format("delta").load("/dbfs/tmp/temp_languages")
languages_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.languages')

# COMMAND ----------

# DBTITLE 1,Locations
locations_df = spark.read.format("delta").load("/dbfs/tmp/temp_locations")
locations_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.locations')

# COMMAND ----------

# DBTITLE 1,Jobs
jobs_df = spark.read.format("delta").load("/dbfs/tmp/temp_jobs")
jobs_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.jobs')

# COMMAND ----------

# DBTITLE 1,JobTranslation
jobtranslation_df = spark.read.format("delta").load("/dbfs/tmp/temp_jobtranslation")
jobtranslation_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.jobtranslation')

# COMMAND ----------

# DBTITLE 1,Holidays
holidays_df = spark.read.format("delta").load("/dbfs/tmp/temp_holidays")
holidays_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.holidays')

# COMMAND ----------

# DBTITLE 1,HolidayLocation
holidaylocation_df = spark.read.format("delta").load("/dbfs/tmp/temp_holidaylocation")
holidaylocation_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.holidaylocation')

# COMMAND ----------

# DBTITLE 1,Users
users_df = spark.read.format("delta").load("/dbfs/tmp/temp_users")
users_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_reservasitios.users')