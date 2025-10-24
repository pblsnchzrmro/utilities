# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists people.gold_reservasitios

# COMMAND ----------

# %sql
# select * from people.silver_reservasitios.departmentsuser

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists people. silver_reservasitios. departmentsuser

# COMMAND ----------

# DBTITLE 1,fact_appointments
# MAGIC %sql
# MAGIC CREATE OR REPLACE table people.gold_reservasitios.fact_appointments AS
# MAGIC SELECT 
# MAGIC   a.Id as IdAppointments,
# MAGIC   CASE WHEN a.Presentado THEN 1 ELSE 0 END as Presentado,
# MAGIC   CASE WHEN a.NoAtendida THEN 0 ELSE 1 END as Atendida,
# MAGIC   CASE WHEN a.CitaEspontanea THEN 1 ELSE 0 END as CitaEspontanea,
# MAGIC   j.Id as IdJob,
# MAGIC   a.IdCitizen as IdUser, -- aun asi no es el de MTE
# MAGIC   cast(a.StartDate as DATE) as Fecha
# MAGIC
# MAGIC
# MAGIC FROM people.silver_reservasitios.appointments a
# MAGIC
# MAGIC LEFT JOIN people.silver_reservasitios.users u 
# MAGIC    ON a.IdUser = u.Id
# MAGIC JOIN people.silver_reservasitios.brand b 
# MAGIC    ON a.IdBrand = b.Id
# MAGIC LEFT JOIN people.silver_reservasitios.jobs j 
# MAGIC   ON a.IdJob = j.Id
# MAGIC LEFT JOIN people.silver_reservasitios.departments d 
# MAGIC   ON j.IdDepartment = d.Id
# MAGIC
# MAGIC WHERE a.IsDeleted = false
# MAGIC AND b.Id = '1'
# MAGIC AND a.Type = '0'
# MAGIC ORDER BY a.Id;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,view_appointment
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE people.gold_reservasitios.view_appointments AS
# MAGIC SELECT 
# MAGIC   CASE WHEN a.Presentado THEN 1 ELSE 0 END as Presentado,
# MAGIC   CASE WHEN a.NoAtendida THEN 0 ELSE 1 END as Atendida,
# MAGIC   CASE WHEN a.CitaEspontanea THEN 1 ELSE 0 END as CitaEspontanea,
# MAGIC   cast(a.StartDate AS DATE) as Fecha, 
# MAGIC   l.Name AS Oficina, 
# MAGIC   a.AddressDepartment as Direccion, 
# MAGIC   a.NameDepartment as Departamento, 
# MAGIC   j.Name as Asiento, 
# MAGIC   concat(trim(a.FirstName), ' ', trim(a.LastName)) as Usuario,
# MAGIC   a.DNI,
# MAGIC   a.Mail as Email,
# MAGIC   a.Notes as Observaciones
# MAGIC   
# MAGIC
# MAGIC FROM people.silver_reservasitios.appointments a
# MAGIC
# MAGIC LEFT JOIN people.silver_reservasitios.jobs j
# MAGIC   on a.IdJob = j.Id
# MAGIC LEFT JOIN people.silver_reservasitios.departments d
# MAGIC   ON j.IdDepartment = d.Id
# MAGIC LEFT JOIN people.silver_reservasitios.locations l 
# MAGIC   ON d.IdLocation = l.Id

# COMMAND ----------

# DBTITLE 1,dim_department
# MAGIC %sql
# MAGIC CREATE OR REPLACE table people.gold_reservasitios.dim_department AS
# MAGIC SELECT 
# MAGIC  d.Id as IdDeparment,
# MAGIC  dt.Name,
# MAGIC  dt.Desc,
# MAGIC  dt.Address,
# MAGIC  CASE WHEN d.IsPrivate THEN 1 ELSE 0 END AS Private,
# MAGIC  l.Id AS LanguageId,
# MAGIC  l.IsoCode,
# MAGIC  lv.Id AS IdLocation
# MAGIC  -- lv.Name AS LocationName
# MAGIC  
# MAGIC FROM people.silver_reservasitios.departments d
# MAGIC
# MAGIC JOIN people.silver_reservasitios.departmenttranslation dt 
# MAGIC    ON d.Id = dt.DepartmentId
# MAGIC JOIN people.silver_reservasitios.languages l 
# MAGIC    ON dt.LanguageId = l.Id
# MAGIC JOIN people.silver_reservasitios.locations lv 
# MAGIC    ON d.IdLocation = lv.Id
# MAGIC WHERE d.IsDeleted = false AND l.IsoCode = 'es'
# MAGIC ORDER BY d.Id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,dim_location
# MAGIC %sql
# MAGIC CREATE OR REPLACE table people.gold_reservasitios.dim_location AS
# MAGIC SELECT 
# MAGIC   l.Id as IdLocation,
# MAGIC   l.Name,
# MAGIC   l.IsPrivate
# MAGIC FROM people.silver_reservasitios.locations l
# MAGIC WHERE l.IsDeleted = false
# MAGIC ORDER BY l.Id;

# COMMAND ----------

# DBTITLE 1,dim_job
# MAGIC %sql
# MAGIC CREATE OR REPLACE table people.gold_reservasitios.dim_job AS
# MAGIC SELECT 
# MAGIC j.Id as IdJob,
# MAGIC jt.Name as Asiento,
# MAGIC -- jt.Desc,
# MAGIC jt.Address as Direccion,
# MAGIC CASE WHEN j.IsPrivate THEN 1 ELSE 0 END as Private,
# MAGIC j.IdDepartment,
# MAGIC -- dv.Name AS Departamento,
# MAGIC l.Id AS IdLenguaje,
# MAGIC l.IsoCode Lenguaje
# MAGIC
# MAGIC
# MAGIC FROM people.silver_reservasitios.jobs j
# MAGIC
# MAGIC LEFT JOIN people.silver_reservasitios.departments dv ON j.IdDepartment = dv.Id
# MAGIC LEFT JOIN people.silver_reservasitios.jobtranslation jt ON j.Id = jt.JobId
# MAGIC LEFT JOIN people.silver_reservasitios.languages l ON jt.LanguageId = l.Id
# MAGIC WHERE j.IsDeleted = false AND l.IsoCode = 'es'
# MAGIC ORDER BY j.IdDepartment, jt.Name;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,dim_holiday
# MAGIC %sql
# MAGIC CREATE OR REPLACE table people.gold_reservasitios.dim_holiday AS
# MAGIC SELECT 
# MAGIC   h.Date,
# MAGIC   l.Name, 
# MAGIC   l.Id as IdLocation
# MAGIC FROM people.silver_reservasitios.holidays h
# MAGIC JOIN people.silver_reservasitios.holidaylocation hl ON h.Id = hl.HolidaysId
# MAGIC JOIN people.silver_reservasitios.locations l ON l.Id = hl.LocationsId
# MAGIC WHERE h.IsDeleted = false AND l.IsDeleted = false
# MAGIC ORDER BY h.Date;

# COMMAND ----------

# DBTITLE 1,dim_usuario
# MAGIC %sql
# MAGIC CREATE OR REPLACE table people.gold_reservasitios.dim_usuario AS
# MAGIC SELECT 
# MAGIC   u.Id AS IdUser,
# MAGIC   concat(trim(u.FirstName), ' ', trim(u.LastName))  AS Usuario, 
# MAGIC   u.Mail as EmailCorporativo,
# MAGIC   u.DNI 
# MAGIC
# MAGIC FROM people.silver_reservasitios.users u
# MAGIC
# MAGIC WHERE u.IsDeleted = false
# MAGIC AND u.Type = 3 