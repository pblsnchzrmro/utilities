# Databricks notebook source
# MAGIC %md
# MAGIC # Transformación 

# COMMAND ----------

# DBTITLE 1,Libraries
# MAGIC %run "/Workspace/data_corporativo/analitico/utils/01Utils"

# COMMAND ----------

# DBTITLE 1,Global constants
# Constantes globales
if len(dbutils.widgets.getAll().items()) != 0:
    running_date = datetime.datetime.strptime(dbutils.widgets.get("running_load_date"), '%Y-%m-%d').date()
else:
    running_date = datetime.datetime.now().date() + dateutil.relativedelta.relativedelta(months= N_PREVIOUS_MONTHS)

mes_carga = '{:02d}'.format(running_date.month) 
anyo_carga = str(running_date.year)

# Dates manipulation
fecha_inicio_mes = get_date_format(anyo_carga, mes_carga, "01")
fecha_fin_mes = get_date_format(anyo_carga, mes_carga, calendar.monthrange(int(anyo_carga),int(mes_carga))[1])

fecha_inicio_ddMMyyyy = get_date_format_slash(anyo_carga, mes_carga, "01")
fecha_fin_ddMMyyyy = get_date_format_slash(anyo_carga, mes_carga, calendar.monthrange(int(anyo_carga),int(mes_carga))[1])
mes_prev = running_date + dateutil.relativedelta.relativedelta(months=-1)
fecha_fin_mes_prev = get_date_format(mes_prev.year, '{:02d}'.format(mes_prev.month), calendar.monthrange(int(mes_prev.year),int(mes_prev.month))[1])
prefix_date = f"{mes_carga}{anyo_carga}"
anyo_carga_anterior = str(int(anyo_carga)-1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Epsilon + KVP

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
df_horas_kvp = DeltaTable.forName(spark, table_name_horas_kvp).toDF().filter(col("FechaCargaFin") == fecha_fin_mes)
df_epsilon = DeltaTable.forName(spark, table_name_epsilon).toDF().filter(col("FechaFinMes") == fecha_fin_mes).select("NIF", "FechaFinMes", "Empresa", "NivelSalarial", "ImpSinBonusNiIncentivos", "TotalSS" )

##############################
####### TRANSFORMACIÓN #######
##############################
# Joininig Epsilon + Horas_KVP by DNI and End of month date.
df = df_horas_kvp.join(df_epsilon, [df_horas_kvp.DNI == df_epsilon.NIF, df_horas_kvp.FechaCargaFin == df_epsilon.FechaFinMes, df_horas_kvp.Cod_Empresa == df_epsilon.Empresa], how="left").select(df_horas_kvp["*"], df_epsilon["*"])

# Columns calculation
df = df.withColumn("Pct_ImpSinBonus", round(df.PORCENTAJE * df["ImpSinBonusNiIncentivos"], 2).cast(FloatType()))
df = df.withColumn("Pct_TotalSS", round(df.PORCENTAJE * df["TotalSS"], 2).cast(FloatType()))
df = df.withColumn("Pct_Bonus", round(df.PORCENTAJE * df["Bonus_del_Mes"], 2).cast(FloatType()))

df = df.withColumn('HOLDING_96400000',when(df.DIM5 == "HOLDING", df.Pct_ImpSinBonus).otherwise(0))
df = df.withColumn('HOLDING_96420000',when(df.DIM5 == "HOLDING", df.Pct_TotalSS).otherwise(0))

df = df.withColumn('MOP_96400000', when( (df.DIM5 == "HOLDING") | (df.DIM5.startswith("V") | df.PROY.startswith("V")), 0).otherwise(df.Pct_ImpSinBonus))
df = df.withColumn('MOP_96420000', when( (df.DIM5 == "HOLDING") | (df.DIM5.startswith("V") | df.PROY.startswith("V")), 0).otherwise(df.Pct_TotalSS))

df = df.withColumn('V_DISF_96400000', when( (df.DIM5.startswith("V")), df.Pct_ImpSinBonus).otherwise(0))
df = df.withColumn('V_DISF_96420000', when( (df.DIM5.startswith("V")), df.Pct_TotalSS).otherwise(0))

# Renaming fields
df = df.withColumnRenamed("Pct_ImpSinBonus", "TOTAL9640")
df = df.withColumnRenamed("Pct_TotalSS", "TOTAL9642")

df = df.withColumn("Total_Refacturacion", round(df["TOTAL9640"] + df["TOTAL9642"] + df["Pct_Bonus"], 2))
df = df.withColumn("DIM5_Proy", concat_ws("-",col("DIM5"),col("DIM5")).cast("string"))
    
df = df.drop("NIF", "FechaFinMes", "FECHA_ACT", "Empresa")
df = df.withColumnRenamed("Cod_Empresa", "Empresa")

# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
# Casting fields
df = df.withColumn("Bonus_del_Mes",df.Bonus_del_Mes.cast(FloatType()))
df = df.withColumn("ImpSinBonusNiIncentivos",df.ImpSinBonusNiIncentivos.cast(FloatType()))
df = df.withColumn("TotalSS",df.TotalSS.cast(FloatType()))
df = df.withColumn("TOTAL9640",df.TOTAL9640.cast(FloatType()))
df = df.withColumn("TOTAL9642",df.TOTAL9642.cast(FloatType()))
df = df.withColumn("Pct_Bonus",df.Pct_Bonus.cast(FloatType()))
df = df.withColumn("HOLDING_96400000",df.HOLDING_96400000.cast(FloatType()))
df = df.withColumn("HOLDING_96420000",df.HOLDING_96420000.cast(FloatType()))
df = df.withColumn("MOP_96400000",df.MOP_96400000.cast(FloatType()))
df = df.withColumn("MOP_96420000",df.MOP_96420000.cast(FloatType()))
df = df.withColumn("V_DISF_96400000",df.V_DISF_96400000.cast(FloatType()))
df = df.withColumn("V_DISF_96420000",df.V_DISF_96420000.cast(FloatType()))
df = df.withColumn("Total_Refacturacion",df.Total_Refacturacion.cast(FloatType()))
df = df.withColumn("PORCENTAJE", df.PORCENTAJE.cast(FloatType()))
df = df.withColumn("DNI", df.DNI.cast(StringType()))   

########################
####### CHECKING #######
########################
check_column_not_null(df, 'DNI')
#check_column_not_zero(df, 'PORCENTAJE')

########################
####### GUARDADO #######
########################
condition = (col("FechaCargaFin") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_Epsilon_KVP, condition, df)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ASTO01

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
df_epsilon_kvp = spark.sql(f"""SELECT FechaCargaFin, DNI, NOMBRE as NombreCliente, DIM5, Empresa, MOP_96400000, V_DISF_96400000 FROM {table_name_Epsilon_KVP} WHERE (NivelSalarial <> 'BECARIO GENERAL' OR NivelSalarial IS NULL) AND FechaCargaFin = '{fecha_fin_mes}'""")
incentivos_final = spark.sql(f"""SELECT CAST(FechaCargaFin AS DATE) AS FechaCargaFin, DNI, DIM5, Empresa, Importe_Decimal, Cuenta_Contable, NOMBRE as NombreCliente FROM {table_name_incentivos} WHERE CAST(FechaCargaFin AS DATE) = '{fecha_fin_mes}'""") 
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df_MOP = df_epsilon_kvp.withColumn('Importe_Decimal', when(df_epsilon_kvp.V_DISF_96400000 == 0, df_epsilon_kvp.MOP_96400000 * 1.1).otherwise(0))
df_VAC = df_epsilon_kvp.withColumn('Importe_Decimal', when(df_epsilon_kvp.V_DISF_96400000 == 0, 0).otherwise(df_epsilon_kvp.V_DISF_96400000))

df_MOP = df_MOP.drop("MOP_96400000", "V_DISF_96400000")
df_VAC = df_VAC.drop("MOP_96400000", "V_DISF_96400000")

df_MOP = df_MOP.withColumn("Cuenta_Contable", lit(cuentas_contables_relacion['Sueldos_Salarios_Directo']))
df_VAC = df_VAC.withColumn("Cuenta_Contable", lit(cuentas_contables_relacion['Vacaciones_Disfrutadas_Directo']))

result_df = df_MOP.union(df_VAC)
result_df = result_df.unionByName(incentivos_final)

result_df = result_df.join(df_asientos, [result_df.Cuenta_Contable == df_asientos.Cuenta_Contable, result_df.FechaCargaFin == df_asientos.Fecha], how="inner").select(result_df["*"], df_asientos["Asiento_OC"].alias("Asiento"))

result_df = result_df.withColumn("Concepto", col("DNI"))
result_df = result_df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
result_df = result_df.withColumn('Cuenta_Contable', result_df.Cuenta_Contable.cast(LongType()))
result_df = result_df.withColumn("Importe_Decimal", result_df.Importe_Decimal.cast(FloatType()))

# Fecha ACT
result_df = result_df.withColumnRenamed("FechaCargaFin", "Fecha")

########################
####### GUARDADO #######
########################
condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_asto_01, condition, result_df)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ASTO02

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
df = spark.sql(f"""SELECT FechaCargaFin, DNI, NOMBRE AS NombreCliente, DIM5, Empresa, MOP_96420000, V_DISF_96420000 FROM {table_name_Epsilon_KVP} WHERE (NivelSalarial <> 'BECARIO GENERAL' OR NivelSalarial IS NULL) AND FechaCargaFin = '{fecha_fin_mes}'""")
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df_MOP = df.withColumn('Importe_Decimal', when(df.MOP_96420000 == 0, 0).otherwise(df.MOP_96420000 * 1.1))
df_VAC = df.withColumn('Importe_Decimal', when(df.V_DISF_96420000 == 0, 0).otherwise(df.V_DISF_96420000))

df_MOP = df_MOP.drop("MOP_96420000", "V_DISF_96420000")
df_VAC = df_VAC.drop("MOP_96420000", "V_DISF_96420000")

df_MOP = df_MOP.withColumn("Cuenta_Contable", lit(cuentas_contables_relacion['Seguridad_Social_Directo']))
df_VAC = df_VAC.withColumn("Cuenta_Contable", lit(cuentas_contables_relacion['Vacaciones_Disfrutadas_SS_Directo']))

df_result = df_MOP.union(df_VAC)

# Join
df_result = df_result.join(df_asientos, [df_result.Cuenta_Contable == df_asientos.Cuenta_Contable, df_result.FechaCargaFin == df_asientos.Fecha], how="inner").select(df_result["*"], df_asientos["Asiento_OC"].alias('Asiento'))

df_result = df_result.withColumnRenamed("FechaCargaFin", "Fecha")
df_result = df_result.withColumn("Concepto", col("DNI"))
df_result = df_result.withColumn('Cuenta_Contable', df_result.Cuenta_Contable.cast(LongType()))

df_result = df_result.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
df_result = df_result.withColumn("Importe_Decimal", df_result.Importe_Decimal.cast(FloatType()))

########################
####### GUARDADO #######
########################
condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_asto_02, condition, df_result)

df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ASTO03

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
# Carga epsilon_kvp
df_epsilon_kvp = spark.sql(f"""SELECT FechaCargaFin, DNI, DIM5, Empresa, NivelSalarial, Pct_Bonus, V_DISF_96400000, Total_Refacturacion, DIM5_Proy, NOMBRE AS NombreCliente FROM {table_name_Epsilon_KVP} WHERE (NivelSalarial <> 'BECARIO GENERAL' OR NivelSalarial IS NULL OR (NivelSalarial == 'BECARIO GENERAL' AND Pct_Bonus > 0)) AND FechaCargaFin = '{fecha_fin_mes}'""").filter( (col('Pct_Bonus') != 0) & (col('Pct_Bonus').isNotNull() ))
# Carga asientos
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")
# Carga traduccion proyectos
traduccionProyectos = DeltaTable.forName(spark, table_name_vacaciones).toDF()

##############################
####### TRANSFORMACIÓN #######
##############################
df = df_epsilon_kvp.withColumn("Cuenta_Contable", lit(cuentas_contables_relacion['Anulacion_Bonus']))
df = df.withColumnRenamed("Pct_Bonus", "Importe_Decimal")
# Join
df = df.join(df_asientos, [df.Cuenta_Contable == df_asientos.Cuenta_Contable, df.FechaCargaFin == df_asientos.Fecha], how="inner").select(df["*"], df_asientos["Asiento_OC"])

df = df.select("Cuenta_Contable", "FechaCargaFin", "Empresa", "Asiento_OC", "Importe_Decimal", "DNI", "DIM5", "NombreCliente")
df = df.withColumnRenamed("Asiento_OC","Asiento")
df = df.withColumnRenamed("FechaCargaFin","Fecha")
df = df.withColumn("Concepto", col("DNI"))

result = df.withColumn("Importe_Decimal", df.Importe_Decimal * -1)
result = result.withColumn('Cuenta_Contable', result.Cuenta_Contable.cast(LongType()))

result = result.join(traduccionProyectos, ["DIM5"], how="left").select(result["*"], traduccionProyectos["Cuenta_generica"])
result = result.withColumn("DIM5", when(col("Cuenta_generica").isNotNull(), col("Cuenta_generica")).otherwise(col("DIM5"))).drop("Cuenta_generica")

# Fecha ACT
result = result.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
result = result.withColumn("Importe_Decimal",result.Importe_Decimal.cast(FloatType()))

########################
####### GUARDADO #######
########################
result.display()
condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_asto_03, condition, result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ASTO N01

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
cc_640 = cuentas_contables_relacion['Sueldos_Salarios_Directo_640']
cc_retr_especie = cuentas_contables_relacion['Retribuciones_En_Especie']
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")
df = spark.sql(f"""SELECT Empresa AS Epsilon_Empresa, Cuenta AS Cuenta_Contable, Fecha, CAST(ImporteDecimal AS FLOAT) AS Importe_Decimal, Varios AS DIM5 FROM {table_name_axapta} WHERE Cuenta IN ({cc_640}, {cc_retr_especie}) AND Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df = df.withColumn('DNI', lit(None))   
df = df.withColumn('Cuenta_Contable', regexp_replace('Cuenta_Contable', cuentas_contables_relacion['Sueldos_Salarios_Directo_640'], cuentas_contables_relacion['Sueldos_Y_Salarios_Interno']))
df = df.withColumn('Cuenta_Contable', regexp_replace('Cuenta_Contable', cuentas_contables_relacion['Retribuciones_En_Especie'], cuentas_contables_relacion['Sueldos_Y_Salarios_Interno']))

df = df.withColumn("asto_01_cc", lit(cuentas_contables_relacion['Sueldos_Salarios_Directo']))
df = df.join(df_asientos, [df.asto_01_cc == df_asientos.Cuenta_Contable, df.Fecha == df_asientos.Fecha], how="inner").select(df["*"], df_asientos.Asiento_OC.alias("Asiento"))
df = df.drop("asto_01_cc")
df = df.withColumn("Cuenta_Contable", df.Cuenta_Contable.cast(IntegerType()))
# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
df.display()
condition = (col("Fecha") >= fecha_inicio_mes) & (col("Fecha") <= fecha_fin_mes)
load_table_unity_catalog(spark, table_name_asto_negativo_01, condition, df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ASTO N02

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
cc_ss_empresa = cuentas_contables_relacion['Seguridad_Social_A_Cargo_De_La_Empresa']
df = spark.sql(f"""SELECT Empresa AS Epsilon_Empresa, Cuenta AS Cuenta_Contable, Fecha, CAST(ImporteDecimal AS FLOAT) AS Importe_Decimal, Varios AS DIM5 FROM {table_name_axapta} WHERE Cuenta IN ({cc_ss_empresa}) AND Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}' AND (Concepto LIKE '%NOMINA%' OR Concepto LIKE '%NOMIMA%' OR Concepto LIKE '%ANULACION PROVISION BONUS%')""")
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df = df.withColumn('Cuenta_Contable', regexp_replace('Cuenta_Contable', cuentas_contables_relacion['Seguridad_Social_A_Cargo_De_La_Empresa'], cuentas_contables_relacion['Seguridad_Social_A_Cargo_De_La_Empresa_Interno']))
df = df.withColumn('DNI', lit(None))

df = df.withColumn("asto_02_cc", lit(cuentas_contables_relacion['Seguridad_Social_Directo']))
df = df.join(df_asientos, [df.asto_02_cc == df_asientos.Cuenta_Contable, df.Fecha == df_asientos.Fecha], how="inner").select(df["*"], df_asientos.Asiento_OC.alias("Asiento"))
df = df.drop("asto_02_cc")
df = df.withColumn("Cuenta_Contable",df.Cuenta_Contable.cast(IntegerType()))

# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))    

########################
####### GUARDADO #######
########################
df.display()
condition = (col("Fecha") >= fecha_inicio_mes) & (col("Fecha") <= fecha_fin_mes)
load_table_unity_catalog(spark, table_name_asto_negativo_02, condition, df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ASTO N03

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
cc_periodificacion_directos = cuentas_contables_relacion['Periodificacion_Directos']
cc_periodificacion_indirectos = cuentas_contables_relacion['Periodificacion_Indirectos']
df = spark.sql(f"""SELECT Empresa AS Epsilon_Empresa, CAST(Cuenta as INT) AS Cuenta_Contable, Fecha, Asiento, CAST(ImporteDecimal AS FLOAT) AS Importe_Decimal, Varios AS DIM5 FROM {table_name_axapta} WHERE (Cuenta = {cc_periodificacion_directos} or Cuenta = {cc_periodificacion_indirectos}) AND Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")
df_bonus = spark.sql(f"""SELECT * FROM {table_name_bonus_anyo_actual} WHERE Fecha = '{fecha_fin_mes}'""")
df_ajuste_vacaciones = spark.sql(f"""SELECT * FROM {table_ajuste_vacaciones}""")
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df = df.withColumn('DNI', lit(None))

df_anulacion_bonus = df_bonus.groupBy("Empresa")\
    .agg(round(sum("Bonus_del_Mes"), 1).alias("Bonus_del_Mes")) \
    .join(df_ajuste_vacaciones, df_bonus.Empresa == df_ajuste_vacaciones.Empresa, how="left") \
    .select(
        lit(cuentas_contables_relacion['Periodificacion_Bonus_Dimensiones']).cast(IntegerType()).alias("Cuenta_Contable"),
        lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
        df_bonus.Empresa.alias("Epsilon_Empresa"),
        (-1 * col("Bonus_del_Mes")).cast(FloatType()).alias("Importe_Decimal"),
        lit(None).alias("DNI"), 
        col("cuenta_generica").alias("DIM5")  
    )

df_anulacion_bonus = df_anulacion_bonus.withColumn("asto_03_cc", lit(cuentas_contables_relacion['Anulacion_Bonus']))

df_anulacion_bonus = df_anulacion_bonus.join(df_asientos, [df_anulacion_bonus.asto_03_cc == df_asientos.Cuenta_Contable, df_anulacion_bonus.Fecha == df_asientos.Fecha], how="inner").select(df_anulacion_bonus["*"], df_asientos.Asiento_OC.alias("Asiento"))
df_anulacion_bonus = df_anulacion_bonus.drop("asto_03_cc")

df = df.unionByName(df_anulacion_bonus)
# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
condition = (col("Fecha") >= fecha_inicio_mes) & (col("Fecha") <= fecha_fin_mes)
load_table_unity_catalog(spark, table_name_asto_negativo_03, condition, df)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### BECARIOS

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
df = spark.sql(f"""SELECT FechaCargaFin AS Fecha, DNI, DIM5, Empresa, NivelSalarial, MOP_96400000, V_DISF_96400000, NOMBRE AS NombreCliente FROM {table_name_Epsilon_KVP} WHERE FechaCargaFin = '{fecha_fin_mes}' AND NivelSalarial = 'BECARIO GENERAL'""")
df_asientos = spark.sql(f"""SELECT * FROM {table_name_asientos} WHERE Fecha = '{fecha_fin_mes}'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df = df.withColumn('Importe_Decimal', when(df.MOP_96400000 == 0, df.V_DISF_96400000).otherwise(df.MOP_96400000).cast(FloatType()))
df.na.drop(subset=["Importe_Decimal"])
df = df.withColumn('Importe_Decimal', -1 * df.Importe_Decimal)

df = df.drop("MOP_96400000", "V_DISF_96400000")
df = df.withColumn("Cuenta_Contable", lit(cuentas_contables_relacion['Sueldos_Salarios_Directo']))

#Joins
df = df.join(df_asientos, [df.Cuenta_Contable == df_asientos.Cuenta_Contable, df.Fecha == df_asientos.Fecha], how="inner").select(df["*"], df_asientos["Asiento_OC"])

df = df.withColumnRenamed("Asiento_OC", "Asiento")
df = df.withColumn('Cuenta_Contable', regexp_replace('Cuenta_Contable', cuentas_contables_relacion['Sueldos_Salarios_Directo'], cuentas_contables_relacion['Becarios_Productivos_962']).cast(IntegerType()))
df = df.withColumn("Concepto", col("DNI"))
# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
df.display()
condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, tabla_name_becarios, condition, df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### BECARIOS NEGATIVO

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
cc_becarios_productivos = cuentas_contables_relacion['Becarios_Productivos']
df = spark.sql(f"""SELECT Empresa, Cuenta AS Cuenta_Contable, Fecha, Asiento, CAST(ImporteDecimal AS FLOAT) AS Importe_Decimal, Varios AS DIM5 FROM {table_name_axapta} WHERE Cuenta IN ({cc_becarios_productivos}) AND Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}' AND Concepto LIKE '%BECARIOS%'""")

##############################
####### TRANSFORMACIÓN #######
##############################
df = df.withColumn('DNI', lit(None))
df = df.withColumn('Cuenta_Contable', regexp_replace('Cuenta_Contable', cuentas_contables_relacion['Becarios_Productivos'], cuentas_contables_relacion['Becarios_Productivos_962']).cast(IntegerType()))
df = df.withColumn('Importe_Decimal', -1 * df.Importe_Decimal)

# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
df.display()
condition = (col("Fecha") >= fecha_inicio_mes) & (col("Fecha") <= fecha_fin_mes)
load_table_unity_catalog(spark, tabla_name_negativos_becarios, condition, df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generación tabla final

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################

# ASTOS
df_ASTO_01 = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Empresa, Asiento, Importe_Decimal, DNI, DIM5, Concepto, NombreCliente FROM {table_name_asto_01} WHERE Fecha = '{fecha_fin_mes}'""")
df_ASTO_02 = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Empresa, Asiento, Importe_Decimal, DNI, DIM5, Concepto, NombreCliente FROM {table_name_asto_02} WHERE Fecha = '{fecha_fin_mes}'""")
df_ASTO_03 = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Empresa, Asiento, Importe_Decimal, DNI, DIM5, Concepto, NombreCliente FROM {table_name_asto_03} WHERE Fecha = '{fecha_fin_mes}'""")
df_ASTO_Becarios = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Empresa, Asiento, Importe_Decimal, DNI, DIM5, Concepto, NombreCliente FROM {tabla_name_becarios} WHERE Fecha = '{fecha_fin_mes}'""")


# ASTO NEGATIVO
df_ASTO_N01 = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, (-1) * Importe_Decimal AS Importe_Decimal, DNI, DIM5, NULL AS Concepto FROM {table_name_asto_negativo_01} WHERE Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")
df_ASTO_N02 = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, (-1) * Importe_Decimal AS Importe_Decimal, DNI, DIM5, NULL AS Concepto FROM {table_name_asto_negativo_02} WHERE Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")
df_ASTO_N03 = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, (-1) * Importe_Decimal AS Importe_Decimal, DNI, DIM5, NULL AS Concepto FROM {table_name_asto_negativo_03} WHERE Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")
df_ASTO_NBecarios = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Empresa, Asiento, Importe_Decimal, DNI, DIM5, NULL AS Concepto FROM {tabla_name_negativos_becarios} WHERE Fecha = '{fecha_fin_mes}'""")

# Carga de fuentes directas
cc_per_directo = cuentas_contables_relacion['Periodificacion_Directos']
cc_per_indirecto = cuentas_contables_relacion['Periodificacion_Indirectos']
df_Obras_Act = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, Importe_Decimal,Concepto, NULL AS DNI, DIM5 FROM {table_name_obra_anio_actual} WHERE Fecha = '{fecha_fin_mes}'""")
df_Obras_Ant = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, Importe_Decimal,Concepto, NULL AS DNI, DIM5 FROM {table_name_obra_curso_anio_anterior} WHERE Fecha = '{fecha_fin_mes}'""")
df_Factu_Int_Ing = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, ImporteDecimal AS Importe_Decimal, Concepto, NULL AS DNI, DIM5 FROM {table_name_ingresos_internos} WHERE Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")
df_Factu_Int_Gas = spark.sql(f"""SELECT Cuenta_Contable, Fecha, Epsilon_Empresa AS Empresa, Asiento, ImporteDecimal AS Importe_Decimal, Concepto, NULL AS DNI, DIM5 FROM {table_name_gastos_internos} WHERE Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")
df_Axapta = spark.sql(f"""SELECT Empresa AS NombreEmpresa, Cuenta, Fecha, Asiento, NombreCliente, NombreProveedor, Divisa, ImporteDecimal, Concepto, Varios FROM {table_name_axapta} WHERE (Cuenta != {cc_per_directo} AND Cuenta != {cc_per_indirecto}) AND Fecha >= '{fecha_inicio_mes}' AND Fecha <= '{fecha_fin_mes}'""")

# Carga de datos de ajuste, cuentas contables y ajuste de vacaciones
df_CC = spark.sql(f"""SELECT IdCuenta, CuentaContable as NombreCuenta, Grupo, NULL AS TIPO, Epigrafe, Subepigrafe, IG, Orden_IG FROM {table_name_cuentas_contables}""")
df_interco = spark.sql(f"""SELECT Cliente_Interco, MIN(Interco) AS Interco FROM {table_name_empresas_interco} GROUP BY Cliente_Interco;""")
df_ajuste_vacaciones = spark.sql(f"""SELECT * FROM {table_ajuste_vacaciones}""")

##############################
####### TRANSFORMACIÓN #######
##############################
# Unions of sources
df = df_ASTO_01.unionByName(df_ASTO_02, allowMissingColumns=True)
df = df.unionByName(df_ASTO_03, allowMissingColumns=True)
df = df.unionByName(df_ASTO_N01, allowMissingColumns=True)
df = df.unionByName(df_ASTO_N02, allowMissingColumns=True)
df = df.unionByName(df_ASTO_N03, allowMissingColumns=True)
df = df.unionByName(df_ASTO_Becarios, allowMissingColumns=True)
df = df.unionByName(df_ASTO_NBecarios, allowMissingColumns=True)
df = df.unionByName(df_Obras_Act, allowMissingColumns=True)
df = df.unionByName(df_Obras_Ant, allowMissingColumns=True)
df = df.unionByName(df_Factu_Int_Ing, allowMissingColumns=True)
df = df.unionByName(df_Factu_Int_Gas, allowMissingColumns=True)

df = df.withColumn("Concepto", substring("Concepto", 1, 550))
    
# Rename
df = df.withColumnRenamed("Importe_Decimal", "ImporteDecimal")
df = df.withColumnRenamed("DIM5", "Varios")
df = df.withColumnRenamed("Cuenta_Contable", "Cuenta")
df = df.withColumnRenamed("Empresa", "NombreEmpresa")

# Reorder
df = df.unionByName(df_Axapta, allowMissingColumns=True)
df = df.select("NombreEmpresa", "Cuenta", "Fecha", "Asiento", "NombreCliente", "NombreProveedor", "Divisa", "ImporteDecimal", "Concepto", "Varios")

df = df.withColumn("Cuenta", df.Cuenta.cast(IntegerType()))
df_CC = df_CC.withColumn("IdCuenta", df_CC.IdCuenta.cast(IntegerType()))

df_CC = df_CC.withColumnRenamed("IdCuenta", "Cuenta_contable")
df_CC = df_CC.withColumnRenamed("CuentaContable", "NombreCuenta")

# Join
df = df.join(df_CC, df.Cuenta == df_CC.Cuenta_contable, how="left").select(df["*"], df_CC["*"])

# Columnas calculadas
# Ingresos en (+)
df = df.withColumn('Ingresos', when(df.IG == "Ingresos", df.ImporteDecimal).otherwise(0))
# Gastos en (-)
df = df.withColumn('Gastos', when(df.IG == "Gastos", -1 * df.ImporteDecimal).otherwise(0))

df = df.withColumn('ResultadosExtraordinarios', when(df.IG == "Resultados extraordinarios", df.ImporteDecimal).otherwise(0))
df = df.withColumn('ResultadosDeterEnajInmov', when(df.IG == "Resultado de deterioros y enajenaciones inmov.", -1 * df.ImporteDecimal).otherwise(0))
df = df.withColumn("EBITDA", df.Ingresos - df.Gastos + df.ResultadosExtraordinarios + df.ResultadosDeterEnajInmov)
df = df.withColumn('Amortizacion', when(df.IG == "Amortización",df.ImporteDecimal).otherwise(0))
df = df.withColumn('PerdidasDetVarProvComerciales', when(df.IG == "Pérdidas, deterioros y variac. Provisiones comerciales",df.ImporteDecimal).otherwise(0))
df = df.withColumn('ResultadosFinancieros', when(df.IG == "Resultados Financieros",df.ImporteDecimal).otherwise(0))

df = df.withColumn('ImporteFinancieroVDisfrutadas',     
    when((df.Asiento.startswith("RR")) & (df.Varios.startswith("V")) & ((df.Cuenta == cuentas_contables_relacion['Vacaciones_Disfrutadas_Directo']) | (df.Cuenta == cuentas_contables_relacion['Vacaciones_Disfrutadas_SS_Directo'])), 0) \
    .when((df.Cuenta == cuentas_contables_relacion['Periodificacion_Indirectos']) | (df.Cuenta == cuentas_contables_relacion['Periodificacion_Directos']) | (df.Cuenta == cuentas_contables_relacion['Anulacion_Bonus']), -1 * df.ImporteDecimal) \
    .when((df.Cuenta == cuentas_contables_relacion['Becarios_Productivos_962']), df.ImporteDecimal) \
    .when(df.Cuenta == cuentas_contables_relacion['Incentivos'], -1 * df.ImporteDecimal)\
    .when(df.Asiento.startswith("RR"), -1 * df.ImporteDecimal)\
    .when(df.IG == "Ingresos", df.ImporteDecimal)\
    .when(df.IG == "Gastos", df.ImporteDecimal)\
    .when(df.IG == "Resultados extraordinarios", df.ImporteDecimal)\
    .when(df.IG == "Amortización", df.ImporteDecimal)\
    .when(df.IG == "Pérdidas, deterioros y variac. Provisiones comerciales", df.ImporteDecimal)\
    .when(df.IG == "Resultados Financieros", df.ImporteDecimal)\
    .when(df.IG == "Resultado de deterioros y enajenaciones inmov.", df.ImporteDecimal)\
    .otherwise(0)
)

df = df.withColumn("EBT", df.EBITDA + df.Amortizacion + df.PerdidasDetVarProvComerciales + df.ResultadosFinancieros)
df = df.withColumn("Anyo", year(df.Fecha))
df = df.withColumn("NombreMes", date_format("Fecha", "MM"))
df = df.replace(to_replace=meses_dict, subset=['NombreMes'])    
df = df.withColumn("Mes", date_format("Fecha", "MM"))

# Calcular el ajuste de vacaciones
auxAjusteASTO1 = df\
    .filter((col("Cuenta") == cuentas_contables_relacion['Sueldos_Y_Salarios_Interno']) | (col("Cuenta") == cuentas_contables_relacion['Sueldos_Salarios_Directo']) | (col("Cuenta") == cuentas_contables_relacion['Incentivos']))\
    .groupBy("NombreEmpresa")\
    .agg(sum("ImporteFinancieroVDisfrutadas").alias("ImporteFinancieroVDisfrutadas"))\
    .persist()

auxAjusteASTO2 = df\
    .filter((col("Cuenta") == cuentas_contables_relacion['Seguridad_Social_A_Cargo_De_La_Empresa_Interno']) | (col("Cuenta") == cuentas_contables_relacion['Seguridad_Social_Directo']))\
    .groupBy("NombreEmpresa").agg(sum("ImporteFinancieroVDisfrutadas").alias("ImporteFinancieroVDisfrutadas"))\
    .persist()

ajuste_vacaciones_asto1 = auxAjusteASTO1\
    .join(df_ajuste_vacaciones, auxAjusteASTO1.NombreEmpresa == df_ajuste_vacaciones.Empresa, how="left") \
    .select(
        col("NombreEmpresa"),
        lit(cuentas_contables_relacion['Sueldos_Salarios_Directo']).cast(IntegerType()).alias("Cuenta"),
        lit(cuentas_contables_relacion['Sueldos_Salarios_Directo']).cast(IntegerType()).alias("Cuenta_contable"), 
        lit("9640100 Sueldos Y Salarios_Directo").alias("NombreCuenta"),
        lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
        lit("RRHH312024").alias("Asiento"),
        lit(None).alias("Cliente"),
        lit(None).alias("NombreCliente"),
        lit(None).alias("Proveedor"),
        lit(None).alias("NombreProveedor"),
        lit(None).alias("Divisa"),
        lit(0).alias("ImporteDecimal"),
        lit("Ajuste Vacaciones").alias("Concepto"),
        col("Ajuste").alias("Varios"),
        lit("2. Gastos personal").alias("Grupo"),
        lit("2.1. Sueldos y salarios").alias("Epigrafe"),
        lit("Sueldos y Salarios").alias("Subepigrafe"),
        lit("Gastos").alias("IG"),
        lit(None).alias("TIPO"),
        lit(0).alias("Ingresos"),
        abs("ImporteFinancieroVDisfrutadas").alias("Gastos"),
        lit(0).alias("ResultadosExtraordinarios"),
        lit(0).alias("ResultadosDeterEnajInmov"),
        abs("ImporteFinancieroVDisfrutadas").alias("EBITDA"),
        lit(0).alias("Amortizacion"),
        lit(0).alias("PerdidasDetVarProvComerciales"),
        lit(0).alias("ResultadosFinancieros"),
        abs("ImporteFinancieroVDisfrutadas").alias("EBT"),
        lit(anyo_carga).alias("Anyo"),
        lit(meses_dict[mes_carga]).alias("NombreMes"),
        lit(mes_carga).alias("Mes"),
        lit(1).alias("Orden_IG"),
        (col("ImporteFinancieroVDisfrutadas") * -1).alias("ImporteFinancieroVDisfrutadas")
    )

df = df.unionByName(ajuste_vacaciones_asto1, allowMissingColumns=True)

ajuste_vacaciones_asto2 = auxAjusteASTO2\
    .join(df_ajuste_vacaciones, auxAjusteASTO2.NombreEmpresa == df_ajuste_vacaciones.Empresa, how="left") \
    .select(
        col("NombreEmpresa"),
        lit(cuentas_contables_relacion['Seguridad_Social_Directo']).cast(IntegerType()).alias("Cuenta"),
        lit(cuentas_contables_relacion['Seguridad_Social_Directo']).cast(IntegerType()).alias("Cuenta_contable"), 
        lit("9642100 Seguridad Social_Directo").alias("NombreCuenta"),
        lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
        lit("RRHH322024").alias("Asiento"),
        lit(None).alias("Cliente"),
        lit(None).alias("NombreCliente"),
        lit(None).alias("Proveedor"),
        lit(None).alias("NombreProveedor"),
        lit(None).alias("Divisa"),
        lit(0).alias("ImporteDecimal"),
        lit("Ajuste Vacaciones").alias("Concepto"),
        col("Ajuste").alias("Varios"),
        lit("2. Gastos personal").alias("Grupo"),
        lit("2.2. Cargas sociales").alias("Epigrafe"),
        lit("Seguridad social a cargo").alias("Subepigrafe"),
        lit("Gastos").alias("IG"),
        lit(None).alias("TIPO"),
        lit(0).alias("Ingresos"),
        abs("ImporteFinancieroVDisfrutadas").alias("Gastos"),
        lit(0).alias("ResultadosExtraordinarios"),
        lit(0).alias("ResultadosDeterEnajInmov"),
        abs("ImporteFinancieroVDisfrutadas").alias("EBITDA"),
        lit(0).alias("Amortizacion"),
        lit(0).alias("PerdidasDetVarProvComerciales"),
        lit(0).alias("ResultadosFinancieros"),
        abs("ImporteFinancieroVDisfrutadas").alias("EBT"),
        lit(anyo_carga).alias("Anyo"),
        lit(meses_dict[mes_carga]).alias("NombreMes"),
        lit(mes_carga).alias("Mes"),
        lit(1).alias("Orden_IG"),
        (col("ImporteFinancieroVDisfrutadas") * -1).alias("ImporteFinancieroVDisfrutadas")
    )

df = df.unionByName(ajuste_vacaciones_asto2, allowMissingColumns=True)

# Interco
df = df.join(df_interco, df.NombreCliente == df_interco.Cliente_Interco, how="left").select(df["*"], df_interco["*"])

df = df.withColumn('Interco',
    when( (col("Interco") == "Interco") , "Interco")\
    .otherwise("Cliente final")
)

# Droping unused columns
df = df.drop("Cliente_Interco", "Cuenta_contable")
# Fecha ACT
df = df.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
# Remove the single quotes
df = df.withColumn("Concepto", regexp_replace("Concepto", "'", ""))

########################
####### GUARDADO #######
########################
df.display()

condition = ((col("Anyo") == anyo_carga) & (col("Mes") == mes_carga))
load_table_unity_catalog(spark, tabla_name_final, condition, df, 'Anyo', 'Mes')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modeled table

# COMMAND ----------

tabla_clientes = spark.sql(f"""
                              SELECT 
                                  IdEmpresa, 
                                  NombreCliente
                              FROM (
                                  SELECT emp.IdEmpresa as IdEmpresa, 
                                  REPLACE(REPLACE(REPLACE(UPPER(NombreCliente), ',', ''), '.', ''), "'", '') AS NombreCliente
                                  FROM {tabla_name_final} tra
                                  LEFT JOIN {table_name_dim_empresas} emp ON lower(tra.NombreEmpresa) = emp.CodEmpresaAxap
                                  GROUP BY emp.IdEmpresa, REPLACE(REPLACE(REPLACE(UPPER(NombreCliente), ',', ''), '.', ''), "'", '')
                                  )
                              WHERE NombreCliente IS NOT NULL AND NombreCliente != ''
                                """
                                )

tabla_proveedores = spark.sql(f"""
                                SELECT 
                                    IdEmpresa, 
                                    NombreProveedor
                                FROM (
                                    SELECT emp.IdEmpresa as IdEmpresa, 
                                    REPLACE(REPLACE(REPLACE(UPPER(NombreProveedor), ',', ''), '.', ''), "'", '') AS NombreProveedor
                                    FROM {tabla_name_final} tra
                                    LEFT JOIN {table_name_dim_empresas} emp ON lower(tra.NombreEmpresa) = emp.CodEmpresaAxap
                                    GROUP BY emp.IdEmpresa, REPLACE(REPLACE(REPLACE(UPPER(NombreProveedor), ',', ''), '.', ''), "'", '')
                                    )
                                WHERE NombreProveedor IS NOT NULL AND NombreProveedor != ''
                                """
                                )

if spark.catalog.tableExists(tabla_proveedores_modeled):
  tabla_proveedores_delta = DeltaTable.forName(spark, tabla_proveedores_modeled)
  (
    tabla_proveedores_delta.alias("tp")
    .merge(
        tabla_proveedores.alias("src"),
        "tp.IdEmpresa = src.IdEmpresa and tp.NombreProveedor = src.NombreProveedor" 
    )
    .whenNotMatchedInsert(values={
        "IdEmpresa": col("src.IdEmpresa"),
        "NombreProveedor": col("src.NombreProveedor")
    })
    .execute()
  )
else:
  tabla_proveedores.write.mode("overwrite").format("delta").saveAsTable(tabla_proveedores_modeled)

if spark.catalog.tableExists(tabla_clientes_modeled):
  tabla_clientes_delta = DeltaTable.forName(spark, tabla_clientes_modeled)
  (
    tabla_clientes_delta.alias("tc")
    .merge(
        tabla_clientes.alias("src"),
        "tc.IdEmpresa = src.IdEmpresa and tc.NombreCliente = src.NombreCliente" 
    )
    .whenNotMatchedInsert(values={
        "IdEmpresa": col("src.IdEmpresa"),
        "NombreCliente": col("src.NombreCliente")
    })
    .execute()
  )
else:
  tabla_clientes.write.mode("overwrite").format("delta").saveAsTable(tabla_clientes_modeled)

# Carga tabla modelada final
tabla_transacciones = spark.sql(f"""
                                SELECT  
                                  Fecha,
                                  CAST(FECHA_ACT AS TIMESTAMP) AS FechaCarga,
                                  emp.IdEmpresa as IdEmpresa,
                                  CAST(Cuenta AS INTEGER) as IdCuenta,
                                  Asiento as Asiento,
                                  Varios as IdProyecto,
                                  CASE
                                    WHEN Concepto LIKE '^[0-9]{8}[a-zA-Z]$'
                                    THEN UPPER(Concepto)
                                    ELSE initcap(Concepto)
                                  END AS Concepto,
                                  CAST(cli.idCliente AS INTEGER) AS IdCliente,
                                  CAST(prov.idProveedor AS INTEGER) AS IdProveedor,
                                  initcap(CONCAT_WS(', ', NULLIF(tf.NombreCliente, ''), NULLIF(tf.NombreProveedor, ''))) as ClienteProveedor,
                                  ImporteFinancieroVDisfrutadas as Importe,
                                  Interco as Tipo,
                                  Anyo,
                                  Mes
                                FROM {tabla_name_final} tf
                                LEFT JOIN {table_name_dim_empresas} emp ON emp.CodEmpresaAxap = lower(tf.NombreEmpresa)
                                LEFT JOIN {tabla_proveedores_modeled} prov ON REPLACE(REPLACE(REPLACE(UPPER(tf.NombreProveedor), ',', ''), '.', ''), "'", '') = prov.NombreProveedor AND emp.IdEmpresa = prov.IdEmpresa
                                LEFT JOIN {tabla_clientes_modeled} cli ON REPLACE(REPLACE(REPLACE(UPPER(tf.NombreCliente), ',', ''), '.', ''), "'", '') = cli.NombreCliente AND emp.IdEmpresa = cli.IdEmpresa
                                WHERE Mes = '{mes_carga}' AND Anyo = '{anyo_carga}'"""
                                )

# It's appended on a condition of a month
condition = ((col("Mes") == mes_carga) & (col("Anyo") == anyo_carga))
load_table_unity_catalog(spark, tabla_final_modeled, condition, tabla_transacciones, 'Anyo', 'Mes')