# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster reserva-sitios

# COMMAND ----------

# MAGIC %pip install sshtunnel
# MAGIC
# MAGIC import pandas as pd
# MAGIC from sshtunnel import SSHTunnelForwarder,logging
# MAGIC import psycopg2 as pg
# MAGIC from pyspark.sql import functions as F
# MAGIC from datetime import datetime, timedelta
# MAGIC from pyspark.sql.window import Window

# COMMAND ----------

# fecha_actual = datetime.now()
# fecha_hace_seis_meses = fecha_actual - timedelta(days=30*6)
# fecha_hace_tres_meses = fecha_actual - timedelta(days=30*3)
# print(fecha_hace_tres_meses)

# COMMAND ----------

Postgre_ssh_host = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-ssh-host")
Postgre_ssh_username = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-ssh-username")
Postgre_ssh_password = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-ssh-password")
Postgre_DB_username = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-DB-username")
Postgre_DB_password = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-DB-password")
Postgre_DB_name = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "ReservaSitios-DB-name")
localhost = '127.0.0.1'

# COMMAND ----------

ssh_tunnel = SSHTunnelForwarder(
    (Postgre_ssh_host, 22),
    ssh_username = Postgre_ssh_username,
    ssh_password = Postgre_ssh_password,
    remote_bind_address = (localhost, 5432)
 )
ssh_tunnel.start()
print("Started tunnel")

# COMMAND ----------

conn = pg.connect(
       host=localhost,
       port=ssh_tunnel.local_bind_port,
       user=Postgre_DB_username,
       password= Postgre_DB_password,
       database=Postgre_DB_name)

# COMMAND ----------

# db_cursor = conn.cursor()
# query = """
# SELECT table_name, grantee, privilege_type
# FROM information_schema.role_table_grants
# WHERE table_schema = 'public';
# """
# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)
# display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC # Tablas a DBFS

# COMMAND ----------

# DBTITLE 1,Appointmentsv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Appointments";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Appointments";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Appointments_df = spark.createDataFrame(df)

Appointments_df = Appointments_df.drop("AdditionalEmails")

# COMMAND ----------

# DBTITLE 1,appointments save
Appointments_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_appointments")

# COMMAND ----------

# DBTITLE 1,Brandv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Brand";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Brand";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Brand_df = spark.createDataFrame(df)

Brand_df = Brand_df.drop("CreationUser").drop("ModifyUser")

# COMMAND ----------

# DBTITLE 1,brand save
Brand_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_brand")

# COMMAND ----------

# DBTITLE 1,Usersv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Users";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Users";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Users_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,users save
Users_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_users")

# COMMAND ----------

# DBTITLE 1,Departmentsv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Departments";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Departments";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Departments_df = spark.createDataFrame(df)
Departments_df = Departments_df.drop("StartTime").drop("EndTime")

# COMMAND ----------

# DBTITLE 1,departments save
Departments_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_departments")

# COMMAND ----------

# DBTITLE 1,DepartmentTranslationv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."DepartmentTranslation";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."DepartmentTranslation";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

DepartmentTranslation_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,departmenttranslation save
DepartmentTranslation_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_departmenttranslation")

# COMMAND ----------

# DBTITLE 1,Languagesv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Languages";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Languages";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Languages_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,languages save
Languages_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_languages")

# COMMAND ----------

# DBTITLE 1,Locationsv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Locations";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Locations";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Locations_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,locations save
Locations_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_locations")

# COMMAND ----------

# DBTITLE 1,Jobsv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Jobs";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Jobs";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

jobs_df = spark.createDataFrame(df)

non_null_columns = [col for col in jobs_df.columns if jobs_df.filter(jobs_df[col].isNotNull()).count() > 0]

jobs_df = jobs_df.select(*non_null_columns)

jobs_df = jobs_df.drop("Time").drop("EmailsToReport")

# COMMAND ----------

# DBTITLE 1,jobs save
jobs_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_jobs")

# COMMAND ----------

# DBTITLE 1,JobTranslationv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."JobTranslation";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."JobTranslation";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

JobTranslation_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,jobtranslation save
JobTranslation_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_jobtranslation")

# COMMAND ----------

# DBTITLE 1,Holidaysv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Holidays";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Holidays";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Holidays_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,holidays save
Holidays_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_holidays")

# COMMAND ----------

# DBTITLE 1,HolidayLocationv2
db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."HolidayLocation";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."HolidayLocation";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

HolidayLocation_df = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,holidaylocation save
HolidayLocation_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_holidaylocation")

# COMMAND ----------

db_cursor = conn.cursor()

query = """
SELECT 
    *
FROM public."Users";
"""

db_cursor.execute(query)

columns = [desc[0] for desc in db_cursor.description]

column_types = []
for desc in db_cursor.description:
    column_types.append((desc[0], desc[1]))

date_type_codes = [1082, 1083, 1114, 1184]
select_parts = []

for col_name, col_type in column_types:
    if col_type in date_type_codes:
        select_parts.append(f'"{col_name}"::text as "{col_name}"')
    else:
        select_parts.append(f'"{col_name}"')

safe_query = f"""
SELECT {', '.join(select_parts)}
FROM public."Users";
"""

db_cursor.execute(safe_query)

rows = db_cursor.fetchall()

df = pd.DataFrame(rows, columns=columns)

Users_df = spark.createDataFrame(df)

Users_df = Users_df.drop("AdditionalEmails")

# COMMAND ----------

Users_df.write.format("delta").mode("overwrite").save("/dbfs/tmp/temp_users")

# COMMAND ----------

# MAGIC %md
# MAGIC # Conexion antigua

# COMMAND ----------

# DBTITLE 1,DepartmentUser
# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()
# query = """SELECT * FROM "DepartmentsUser_View";"""
# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)
# display(df)  

# COMMAND ----------

# DBTITLE 1,Departments
# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()
# query = """SELECT "Id", "Name",coalesce(CAST("Desc" AS text), 'NULL') AS "Desc", "Address", "IsPrivate", "LanguageId", "IsoCode",
#        "LocationId", "LocationName" FROM "Departments_View";"""
# df = pd.read_sql_query(query, conn)
# df.rename(columns = {'Desc':'Descrip'}, inplace = True)
# df = spark.createDataFrame(df)
# df.display() # Reserva.Departments


# COMMAND ----------

# DBTITLE 1,Locations
# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()
# query = """SELECT * FROM "Locations_View";"""
# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)
# df.display() # Reserva.Locations



# COMMAND ----------

# DBTITLE 1,Appointments
# ## RECARGA TABLA ENTERA ##

# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()
# query = """select "Id", "CreationDate","CreationUser", coalesce(CAST("StartDate" AS text), 'NULL') AS "StartDate", coalesce(CAST("EndDate" AS text), 'NULL') AS "EndDate",
# "Presentado",coalesce(CAST("PresentadoDate" AS text), 'NULL') AS "PresentadoDate",coalesce(CAST("RealStartDate" AS text), 'NULL') AS "RealStartDate","RealEndDate","NoAtendida","CitaEspontanea","IdJob","NameJob","AddressJob","IdUser","UsuarioAgente","Usuario","Mail","IdBrand","Name",
#  coalesce(CAST("IdRecurrent" AS text), 'NULL') AS "IdRecurrent", coalesce(CAST("MotivoAusencia" AS text), 'NULL') AS "MotivoAusencia", "ModifyDate" from "Appointments_View";"""
# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)
# # Insert Dataframe into SQL:
# df = df.withColumn("FECHA_ACT", F.date_format(F.expr("from_utc_timestamp(current_timestamp(), 'Europe/Madrid')"), "yyyy-MM-dd HH:mm:ss"))
# df.display()



# COMMAND ----------

# # Obtenemos la última fecha de modificación que tenemos en Azure SQL para traer solo los nuevos valores
# df_max = extract_Azure_SQL('[RESERVA].[Appointments]')
# max_timestamp = df_max.agg(F.max(F.col("ModifyDate")).alias("max_timestamp")).collect()[0]["max_timestamp"]
# max_timestamp = max_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
# max_timestamp = str(max_timestamp + "+0000")
# print(max_timestamp)

# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()

# query = f"""select "Id", "CreationDate","CreationUser", coalesce(CAST("StartDate" AS text), 'NULL') AS "StartDate", coalesce(CAST("EndDate" AS text), 'NULL') AS "EndDate",
# "Presentado",coalesce(CAST("PresentadoDate" AS text), 'NULL') AS "PresentadoDate",coalesce(CAST("RealStartDate" AS text), 'NULL') AS "RealStartDate","RealEndDate","NoAtendida","CitaEspontanea","IdJob","NameJob","AddressJob","IdUser","UsuarioAgente","Usuario","Mail","IdBrand","Name",
#  coalesce(CAST("IdRecurrent" AS text), 'NULL') AS "IdRecurrent", coalesce(CAST("MotivoAusencia" AS text), 'NULL') AS "MotivoAusencia", "ModifyDate" from "Appointments_View"
#  WHERE "Appointments_View"."ModifyDate" >= '{max_timestamp}';"""

# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)
# df = df.withColumn("RealEndDate", F.col("RealEndDate").cast("string"))

# # Insert Dataframe into SQL:
# df = df.withColumn("FECHA_ACT", F.date_format(F.expr("from_utc_timestamp(current_timestamp(), 'Europe/Madrid')"), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# # Cargar nuevos datos en la tabla temporal
# insert_Azure_SQL(df,"[RESERVA].[TMP_Appointments]",mode="append")

# connAz = db_conn()
# cursorAz = connAz.cursor()
# cursorAz.execute("""
# MERGE INTO [RESERVA].[Appointments] AS Target
# USING [RESERVA].[TMP_Appointments] AS Source
# ON Target.Id = Source.Id -- Condición para identificar coincidencias
# WHEN MATCHED THEN
#     UPDATE SET
#         Target.CreationDate = Source.CreationDate,
#         Target.CreationUser = Source.CreationUser,
#         Target.StartDate = Source.StartDate,
#         Target.EndDate = Source.EndDate,
#         Target.Presentado = Source.Presentado,
#         Target.PresentadoDate = Source.PresentadoDate,
#         Target.RealStartDate = Source.RealStartDate,
#         Target.RealEndDate = Source.RealEndDate,
#         Target.NoAtendida = Source.NoAtendida,
#         Target.CitaEspontanea = Source.CitaEspontanea,
#         Target.IdJob = Source.IdJob,
#         Target.NameJob = Source.NameJob,
#         Target.AddressJob = Source.AddressJob,
#         Target.IdUser = Source.IdUser,
#         Target.UsuarioAgente = Source.UsuarioAgente,
#         Target.Usuario = Source.Usuario,
#         Target.Mail = Source.Mail,
#         Target.IdBrand = Source.IdBrand,
#         Target.Name = Source.Name,
#         Target.IdRecurrent = Source.IdRecurrent,
#         Target.MotivoAusencia = Source.MotivoAusencia,
#         Target.ModifyDate = Source.ModifyDate,
#         Target.FECHA_ACT = Source.FECHA_ACT
# WHEN NOT MATCHED BY TARGET THEN
#     INSERT (Id, CreationDate, CreationUser, StartDate, EndDate, Presentado, PresentadoDate, RealStartDate, RealEndDate, 
#             NoAtendida, CitaEspontanea, IdJob, NameJob, AddressJob, IdUser, UsuarioAgente, Usuario, Mail, IdBrand, 
#             Name, IdRecurrent, MotivoAusencia, ModifyDate, FECHA_ACT)
#     VALUES (Source.Id, Source.CreationDate, Source.CreationUser, Source.StartDate, Source.EndDate, Source.Presentado, 
#             Source.PresentadoDate, Source.RealStartDate, Source.RealEndDate, Source.NoAtendida, Source.CitaEspontanea, 
#             Source.IdJob, Source.NameJob, Source.AddressJob, Source.IdUser, Source.UsuarioAgente, Source.Usuario, 
#             Source.Mail, Source.IdBrand, Source.Name, Source.IdRecurrent, Source.MotivoAusencia, Source.ModifyDate, 
#             Source.FECHA_ACT)
#     WHEN NOT MATCHED BY SOURCE THEN
#     DELETE;
#    """)

# print(f"Número de filas afectadas: {cursorAz.rowcount}")           
# connAz.commit()
# connAz.close()

# COMMAND ----------

# # Nos traemos los datos para encontrar los usuarios que tengan duplicados las entradas para un mismo día
# df_full = extract_Azure_SQL('[RESERVA].[Appointments]')
# df_full = df_full.filter(F.col("presentadoDate") != "NULL")

# # Creamos nuevo campo para limpiar presentadoDate
# df_full = df_full.withColumn("presentadoDateNew", F.substring(F.col("presentadoDate"), 1, 10))
# df_full = df_full.filter(~F.col("presentadoDateNew").substr(1, 4).cast("integer").isin(1))
# df_full = df_full.withColumn("presentadoDateNew", F.to_date(F.col("presentadoDateNew"), "yyyy-MM-dd"))

# # Crear una ventana particionada por Mail y presentadoDateNew, y contar el número de registros por día
# ventana = Window.partitionBy("Mail", "presentadoDateNew")
# df_full = df_full.withColumn("registros_por_dia", F.count("presentadoDateNew").over(ventana))

# # Filtramos los usuarios que se han registrado más de una vez el mismo día
# df_filtrado = df_full.filter(F.col("registros_por_dia") >= 2)

# # Seleccionamos el registro más antiguo de los dos registros del mismo día
# df_mas_antiguo = df_filtrado.withColumn("registro_mas_antiguo", F.first("PresentadoDate").over(ventana))
# df_mas_antiguo = df_mas_antiguo.filter(F.col("PresentadoDate") == F.col("registro_mas_antiguo"))

# # Obtenemos los Id para eliminar
# lista_ids = df_mas_antiguo.rdd.map(lambda x: x["Id"]).collect()

# if lista_ids:
#     connAz = db_conn()
#     cursorAz = connAz.cursor()
#     # Crear la consulta SQL con parámetros
#     consulta_sql = "DELETE FROM RESERVA.Appointments WHERE Id IN ({})".format(', '.join(map(str, lista_ids)))
#     cursorAz.execute(consulta_sql)
#     # Imprimir la consulta antes de ejecutarla
#     print(f"Número de filas afectadas: {cursorAz.rowcount}")
#     connAz.commit()
#     connAz.close()

# else:
#     print("La variable está vacía o es None.")

# COMMAND ----------

# DBTITLE 1,Jobs
# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()
# query = """SELECT "Id", "Name", "IdDepartment", "NameDepartment", "IsPrivate" FROM "Jobs_View";"""
# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)

# df.display()

# COMMAND ----------

# DBTITLE 1,Holidays
# print('Executing SQL Query & Fetching Results...')
# db_cursor = conn.cursor()
# query = """SELECT * FROM "Holidays_View";"""
# df = pd.read_sql_query(query, conn)
# df = spark.createDataFrame(df)

# df.display()


# COMMAND ----------

db_cursor.close()
ssh_tunnel.close()