# Databricks notebook source
# MAGIC %md
# MAGIC # Extracción de orígenes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instrucciones de ejecución
# MAGIC Ejecuta cada celda para realizar la extracción de cada fichero en su correspondiente tabla del catálogo unitario.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parámetros de ejecución

# COMMAND ----------

# MAGIC %run "/Workspace/data_corporativo/analitico/utils/01Utils"

# COMMAND ----------

# DBTITLE 1,Dates manipulation
# Fecha de ejecución
if len(dbutils.widgets.getAll().items()) != 0:
    running_date = datetime.datetime.strptime(dbutils.widgets.get("running_load_date"), '%Y-%m-%d').date()
    carga_dim_empresa = dbutils.widgets.get("carga_dim_empresa")
    carga_datos_historicos = dbutils.widgets.get("carga_datos_historicos")
    carga_Axapta = dbutils.widgets.get("carga_Axapta")
    carga_B_Año_Act = dbutils.widgets.get("carga_B_Anyo_Act")
    carga_B_Año_Ant = dbutils.widgets.get("carga_B_Anyo_Ant")
    carga_Epsilon = dbutils.widgets.get("carga_Epsilon")
    carga_incentivos = dbutils.widgets.get("carga_incentivos")
    carga_asientos = dbutils.widgets.get("carga_asientos")
    carga_Horas_KVP = dbutils.widgets.get("carga_Horas_KVP")
    carga_facturacion_interna = dbutils.widgets.get("carga_facturacion_interna")
    carga_obra_año_act = dbutils.widgets.get("carga_obra_anyo_act")
    carga_obra_año_anterior = dbutils.widgets.get("carga_obra_anyo_anterior")
    carga_tablas = dbutils.widgets.get("carga_tablas")
else:
    running_date = datetime.datetime.now().date()
    carga_datos_historicos = "N"
    carga_dim_empresa = "N"
    carga_Axapta = "Y"
    carga_B_Año_Act = "Y"
    carga_B_Año_Ant = "Y"
    carga_Epsilon = "Y"
    carga_Horas_KVP = "Y"
    carga_facturacion_interna = "Y"
    carga_obra_año_act = "Y"
    carga_obra_año_anterior = "Y"
    carga_incentivos = "Y"
    carga_asientos = "Y"
    carga_tablas = "Y"

# Mes y año de ejecución concreto del algoritmo en formato string.
previous_month = running_date + dateutil.relativedelta.relativedelta(months = N_PREVIOUS_MONTHS)
mes_carga = '{:02d}'.format(previous_month.month) 
anyo_carga = str(previous_month.year)

# Fecha inicio y fin de mes en formato 
fecha_inicio_mes = get_date_format(anyo_carga, mes_carga, "01")
fecha_fin_mes = get_date_format(anyo_carga, mes_carga, calendar.monthrange(int(anyo_carga),int(mes_carga))[1])

# Fecha inicio y fin de mes en formato slash
fecha_inicio_ddMMyyyy = get_date_format_slash(anyo_carga, mes_carga, "01")
fecha_fin_ddMMyyyy = get_date_format_slash(anyo_carga, mes_carga, calendar.monthrange(int(anyo_carga),int(mes_carga))[1])

mes_prev = previous_month + dateutil.relativedelta.relativedelta(months=-1)
fecha_fin_mes_prev = get_date_format(mes_prev.year, '{:02d}'.format(mes_prev.month), calendar.monthrange(int(mes_prev.year),int(mes_prev.month))[1])
mes_prev_string = f"{mes_prev.month:02d}"
prefix_date = f"{mes_carga}{anyo_carga}"
anyo_carga_anterior = str(int(anyo_carga)-1)

# COMMAND ----------

# DBTITLE 1,Mounting points files
# Nombre de los ficheros excel.
table_file_name_transacciones_axapta = f"{mes_carga}{anyo_carga} TRANSACCIONES AX.xlsx"
table_file_name_bonus_actual = f"{mes_carga}{anyo_carga} BONUS {anyo_carga}.xlsx"
table_file_name_bonus_anterior = f"{mes_carga}{anyo_carga} BONUS {int(anyo_carga)-1}.xlsx"
table_file_name_epsilon = f"{mes_carga}{anyo_carga} COSTES.xlsx"
table_file_name_incentivos = f"{mes_carga}{anyo_carga} INCENTIVOS.xlsx"
table_file_name_vacaciones = "Proyectos vacaciones y genéricas.xlsx"
table_file_name_cuentas_contables = "Maestro PYG.xlsx"
table_file_name_empresas_interco = "Listado empresas interco.xlsx"
table_file_name_dim_empresas = "Nombre empresas.xlsx"
table_file_name_nombre_asientos = f"Nombres asientos {anyo_carga}.xlsx"
table_file_name_transacciones = f"transacciones {anyo_carga}.xlsx"

# Directory names (for multiple companies)
kvp_mount_point = f"{mounting_point}Horas_KVP/{anyo_carga}"
obra_curso_anterior_mount_point = f"{mounting_point}Obra_curso_año_anterior/{anyo_carga}"
oc_mount_point = f"{mounting_point}Obra_curso_año_actual/{anyo_carga}"

# Directory names (single file)
axapta_mount_point = f"{mounting_point}Transacciones AX/{anyo_carga}/TRANSACCIONES CONSOL AX/{table_file_name_transacciones_axapta}"
bonus_actual_mount_point = f"{mounting_point}Bonus_Act/{anyo_carga}/{table_file_name_bonus_actual}"
bonus_anterior_point_name = f"{mounting_point}Bonus_Ant/{anyo_carga}/{table_file_name_bonus_anterior}"
epsilon_mount_point = f"{mounting_point}RRHH_Epsilon/{anyo_carga}/{table_file_name_epsilon}"
incentivos_mount_point = f"{mounting_point}Incentivos/{anyo_carga}/{table_file_name_incentivos}"
mount_point_name_vacaciones = f"{mounting_point}Tablas/{table_file_name_vacaciones}"
mount_point_name_cuentas_contables = f"{mounting_point}Tablas/{table_file_name_cuentas_contables}"
mount_point_name_empresas_interco = f"{mounting_point}Tablas/{table_file_name_empresas_interco}"
mount_point_name_dim_empresas = f"{mounting_point}Tablas/{table_file_name_dim_empresas}"
mount_point_name_asientos = f"{mounting_point}Nombre Asientos/{table_file_name_nombre_asientos}"
mount_point_historico_transacciones = f"{mounting_point}historico_transacciones/{table_file_name_transacciones}"

# Spark mount points.
path_spark_asientos = "dbfs:" + mount_point_name_asientos
path_spark_transacciones_axapta = "dbfs:" + axapta_mount_point
path_spark_bonus_anio_actual = "dbfs:" + bonus_actual_mount_point
path_spark_bonus_anio_anterior = "dbfs:" + bonus_anterior_point_name
path_spark_epsilon = "dbfs:" + epsilon_mount_point
path_spark_dim_empresas = "dbfs:" + mount_point_name_dim_empresas
spark_path_incentivos = "dbfs:" + incentivos_mount_point
path_spark_vacaciones = "dbfs:" + mount_point_name_vacaciones
path_spark_cuentas_contables = "dbfs:" + mount_point_name_cuentas_contables
path_spark_empresas_interco = "dbfs:" + mount_point_name_empresas_interco
spark_path_historico_transacciones = "dbfs:" + mount_point_historico_transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transacciones Axapta

# COMMAND ----------

# DBTITLE 1,Transacciones LOAD
##############################
####### CARGA ORIGENES #######
##############################
result = spark.read.format(file_system).option(header_str, "true").option("maxRowsInMemory", 25).load(path_spark_transacciones_axapta)

##############################
####### TRANSFORMACIÓN #######
##############################
result = result \
        .filter((to_date(col(fecha_str), "M/d/yy") >= to_date(lit(fecha_inicio_mes), "yyyy-MM-dd")) & (to_date(col(fecha_str), "M/d/yy") <= to_date(lit(fecha_fin_mes), "yyyy-MM-dd"))) \
        .fillna(value='', subset=["Concepto"]) \
        .filter(~(upper("Concepto").contains(f"OBRA EN CURSO")) & ~(upper("Concepto").contains(f"FACTURACION ANTICIPADA"))) \
        .select(
            lower(col("Empresa")).alias("Empresa"),
            regexp_replace(col("NombreEmpresa"), r',', '').alias("NombreEmpresa"),
            col("Cuenta").cast(IntegerType()).alias("Cuenta"),
            col("NombreCuenta"),
            to_date(col(fecha_str), "M/d/yy").alias("Fecha"),
            col("Asiento"),
            col("Cliente").cast(IntegerType()).cast(StringType()).alias("Cliente"),
            col("NombreCliente"),
            col("Proveedor").cast(IntegerType()).alias("Proveedor"),
            col("NombreProveedor"),
            col("Divisa"),
            (regexp_replace(col("Importe_Euros"), ",", "").cast(FloatType()) * -1).alias("ImporteDecimal"),
            col("Concepto"),
            col("Varios")
        )

result = result.withColumn(fecha_act_str, date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
result.display()

condition = (date_format(col(fecha_str), "yyyy-MM-dd") >= fecha_inicio_mes) & (date_format(col(fecha_str), "yyyy-MM-dd") <= fecha_fin_mes)
load_table_unity_catalog(spark, table_name_axapta, condition, result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus año actual
# MAGIC Relación del bonus recibido en el año actual hasta el mes presente de cada empleado de cada empresa Hiberus

# COMMAND ----------

# DBTITLE 1,Bonus año actual LOAD
##############################
####### CARGA ORIGENES #######
##############################
sheet_names = get_column_names(bonus_actual_mount_point)
sheet_names = [sheet for sheet in sheet_names if "INCIDENCIAS" not in sheet]

dataframe_unions = []

for sheet in sheet_names:
    sheet_look = f"'{sheet}'!A2"
    df_spark = spark.read.format(file_system).option(header_str, "true").option("maxRowsInMemory", 100).option(data_add_str, sheet_look).load(path_spark_bonus_anio_actual)

    if not df_spark.isEmpty():

        # Se quitan todos los espacios de las columnas a la izq y derecha 
        columns = df_spark.columns
        trimmed_columns = []
        cols_some = []
        for column in columns:
            if not column.startswith("_"):
                trimmed_columns.append(column.strip())
                cols_some.append(column)

        for old_col, new_col in zip(cols_some, trimmed_columns):
            df_spark = df_spark.withColumnRenamed(old_col, new_col)

        # Se seleccionan solo las columnas necesarias
        df_spark = df_spark.select(
            lit(sheet.lower()).alias(empresa_str),
            upper(regexp_replace(col("DNI"), ' ', '')).alias("DNI"),
            col("Nombre"),
            regexp_replace(col("Enero"), ",", "").cast(FloatType()).alias("Enero"), 
            regexp_replace(col("Febrero"), ",", "").cast(FloatType()).alias("Febrero"),
            regexp_replace(col("Marzo"), ",", "").cast(FloatType()).alias("Marzo"), 
            regexp_replace(col("Abril"), ",", "").cast(FloatType()).alias("Abril"), 
            regexp_replace(col("Mayo"), ",", "").cast(FloatType()).alias("Mayo"), 
            regexp_replace(col("Junio"), ",", "").cast(FloatType()).alias("Junio"), 
            regexp_replace(col("Julio"), ",", "").cast(FloatType()).alias("Julio"), 
            regexp_replace(col("Agosto"), ",", "").cast(FloatType()).alias("Agosto"), 
            regexp_replace(col("Septiembre"), ",", "").cast(FloatType()).alias("Septiembre"), 
            regexp_replace(col("Octubre"), ",", "").cast(FloatType()).alias("Octubre"), 
            regexp_replace(col("Noviembre"), ",", "").cast(FloatType()).alias("Noviembre"), 
            regexp_replace(col("Diciembre"), ",", "").cast(FloatType()).alias("Diciembre")
        )

        dataframe_unions.append(df_spark)

df_bonus_act = reduce(DataFrame.union, dataframe_unions)

##############################
####### TRANSFORMACIÓN #######
##############################
# Unpivot
result = df_bonus_act.unpivot(
    ['DNI', "Nombre", "Empresa"], 
    ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"],
    "Atributo", "Bonus_del_Mes"
    )

# Generating last day of the month as Date.
result = result.replace(to_replace=months_dict, subset=['Atributo'])
result = result.withColumn(
    "Fecha",
    last_day(to_date(concat_ws("-", lit(anyo_carga), lit(mes_carga), lit(1))))
)
result = result.drop("Atributo")

# Cambio tipos
result = result.withColumn("Bonus_del_Mes", regexp_replace(col("Bonus_del_Mes"), ",", "").cast(FloatType()))
result = result.na.fill(value=0,subset=["Bonus_del_Mes"])
result = result.filter(col("DNI").isNotNull())
result = result.dropDuplicates()

# Group by para eliminar los bonus que son 0
result = result.groupBy("DNI", "Nombre", fecha_str, empresa_str).agg(sum("Bonus_del_Mes").alias("Bonus_del_Mes"))
result = result.withColumn("Bonus_del_Mes", result.Bonus_del_Mes.cast(FloatType()))
result = result.withColumn("Nombre", result.Nombre.cast(StringType()))

# Fecha ACT
result = result.withColumn("FECHA_ACT", date_format(current_timestamp(), timestamp_act_format))

########################
####### GUARDADO #######
########################
condition = (col(fecha_str) == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_bonus_anyo_actual, condition, result)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Horas KVP
# MAGIC Relación del porcentaje de horas dedicado a cada proyecto por trabajador.

# COMMAND ----------

# DBTITLE 1,Horas KVP LOAD
##############################
####### CARGA ORIGENES #######
##############################
xlsx_file_list = search_excel_with_company(kvp_mount_point, prefix_date)
df_bonus_anyo_actual = DeltaTable.forName(spark, table_name_bonus_anyo_actual).toDF().filter(col("Fecha") == fecha_fin_mes)

dataframe_unions = []
for file_path, empresa in xlsx_file_list:

    sheet_selected = [x for x in get_column_names(file_path.replace('dbfs:', '')) if x.lower() != 'dni'][0]
    result = spark.read.format(file_system).option(header_str, "true").option(data_add_str, f"'{sheet_selected}'!").load(file_path)

    #result.display()

    columns = result.columns
    columns_renamed = [''.join([i for i in x if not i.isdigit()]) for x in columns]
    for col, renamed in zip(columns, columns_renamed):
        result = result.withColumnRenamed(col, renamed)

    from pyspark.sql.functions import col, trim, lower
    result = result.select(
        last_day(lit(f"{anyo_carga}-{mes_carga}-01")).alias("FechaCargaFin"),
        lower(lit(empresa.split()[0])).alias("Cod_Empresa"),
        upper(col("dni")).alias("DNI"),
        col("NOMBRE"), 
        col("DIM").alias("DIM5"),
        when(col("REFACTURACIÓN") == '', None).otherwise(col("REFACTURACIÓN")).alias("PROY"),
        col("HORAS")
    )

    dataframe_unions.append(result)

kvp_data = reduce(DataFrame.union, dataframe_unions)

##############################
####### TRANSFORMACIÓN #######
##############################
result = kvp_data.join(df_bonus_anyo_actual, [kvp_data.DNI == df_bonus_anyo_actual.DNI, kvp_data.FechaCargaFin == df_bonus_anyo_actual.Fecha, kvp_data.Cod_Empresa == df_bonus_anyo_actual.Empresa], how="left").select(kvp_data["*"], df_bonus_anyo_actual["Bonus_del_Mes"])

result = result.na.fill(value=0,subset=["Bonus_del_Mes"])
result = result.withColumn("HORAS", col("HORAS").cast(DecimalType(20, 17)).alias("HORAS"))
result = result.dropDuplicates()
aux = result.groupBy("Cod_Empresa", "DNI").agg(sum("HORAS").alias("horas_total"))
result = result.join(aux, on=['Cod_Empresa', 'DNI']).withColumn("PORCENTAJE", (col("HORAS")/col("horas_total")).cast(DecimalType(20, 17)))
result = result.drop("horas_total")
result = result.drop("HORAS")

#result = result.withColumnRenamed("REFACTURACIÓN", "REFACTURACION")

result = result.filter(col("DNI") != "NaN")

# Cambiar tipo
result = result.withColumn("Bonus_del_Mes", result.Bonus_del_Mes.cast(FloatType()))
result = result.na.fill(value=0,subset=["PORCENTAJE"])
# Fecha ACT
result = result.withColumn(fecha_act_str, date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))  

########################
####### GUARDADO #######
########################
condition = (col("FechaCargaFin") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_horas_kvp, condition, result)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus año anterior
# MAGIC Bonus del año anterior de cada empleado. Si el mes presente es a partir de septiembre, no se carga el fichero.

# COMMAND ----------

# DBTITLE 1,Bonus año anterior LOAD
##############################
####### CARGA ORIGENES #######
##############################
if file_exists(bonus_anterior_point_name):
    df_companies = spark.sql(f"SELECT CodEmpresaAxap FROM {table_name_dim_empresas} WHERE CodEmpresaAxap != 'H126'")
    sheet_names = get_column_names(bonus_anterior_point_name)
    sheet_names = [sheet for sheet in sheet_names if (("INCIDENCIAS" not in sheet) and ("Bonus Contratado" not in sheet))]

    dataframe_unions = []
    for sheet in sheet_names:
        df_spark = spark.read.format(file_system).option("dataAddress", f"'{sheet}'!A2").option("maxRowsInMemory", 25).option("header", "true").load(path_spark_bonus_anio_anterior)
        
        if not df_spark.isEmpty():
            # Se seleccionan solo las columnas necesarias
            df_spark = df_spark \
                .filter(col("DNI").isNotNull())\
                .select(
                    upper(col("DNI")).alias("DNI"),
                    lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
                    regexp_replace(col(f"ASTO {mes_carga}"), ",", "").cast(FloatType()).alias("Bonus_AA")
                )
            df_spark = df_spark.withColumn("Empresa", lit(sheet.lower()))
            dataframe_unions.append(df_spark)

    df_bonus_ant = reduce(DataFrame.union, dataframe_unions)

    ##############################
    ####### TRANSFORMACIÓN #######
    ##############################
    df_bonus_ant = df_bonus_ant.fillna(value=0,subset=["Bonus_AA"])
    df_bonus_ant = df_bonus_ant.join(df_companies, [df_bonus_ant.Empresa == df_companies.CodEmpresaAxap], how="inner").select(df_bonus_ant["*"])

    ########################
    ####### GUARDADO #######
    ########################
    df_bonus_ant.display()

    condition = (col("Fecha") == fecha_fin_mes)
    load_table_unity_catalog(spark, table_name_bonus_anio_anterior, condition, df_bonus_ant)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Epsilon
# MAGIC Importe de los salarios de cada trabajador junto a las cotizaciones, deducciones etc.

# COMMAND ----------

# DBTITLE 1,Epsilon LOAD
##############################
####### CARGA ORIGENES #######
##############################
result = spark.read.format(file_system).option("header", "true").load(path_spark_epsilon)
df_companies = spark.sql(f"SELECT CodEmpresaAxap FROM {table_name_dim_empresas} WHERE CodEmpresaAxap != 'H126'")

##############################
####### TRANSFORMACIÓN #######
##############################
result_columns = result.columns
to_delete_cols = ['Horas Complemen', 'Horas Extras', 'Total', 'Disponibilidad', 'Plus Domingo', 'Plus Festivos', 'Plus Noche', 'Source']

for col in to_delete_cols:
    if col in result_columns:
        result = result.drop(col)

# Fecha
result = result.withColumn("FechaFinMes", lit(fecha_fin_mes).cast("date"))
# # Eliminamos registros nulos en Empresa para las filas extrañas que puedan haber quedado en el Excel
from pyspark.sql.functions import lower, col
result = result.na.drop(subset=["Empresa"])

# Rename and remove nulls
result = result.withColumnRenamed("Porcentaje Jornada", "PorcentajeJornada")
result = result.withColumn("PorcentajeJornada", regexp_replace(regexp_replace("PorcentajeJornada", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Importe Bruto","02_IMPORTE_BRUTO")
result = result.withColumn("02_IMPORTE_BRUTO", regexp_replace(regexp_replace("02_IMPORTE_BRUTO", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Deducciones","03_DEDUCCIONES")
result = result.withColumn("03_DEDUCCIONES", regexp_replace(regexp_replace("03_DEDUCCIONES", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("S.S. Operario","04_SS_OPERARIO")
result = result.withColumn("04_SS_OPERARIO", regexp_replace(regexp_replace("04_SS_OPERARIO", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Cuota IRPF","05_CUOTA_IRPF")
result = result.withColumn("05_CUOTA_IRPF", regexp_replace(regexp_replace("05_CUOTA_IRPF", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Líquido","06_LIQUIDO")
result = result.withColumn("06_LIQUIDO", regexp_replace(regexp_replace("06_LIQUIDO", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("S.S. Empresa","07_SS_EMPRESA")
result = result.withColumn("07_SS_EMPRESA", regexp_replace(regexp_replace("07_SS_EMPRESA", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Pagos Delegados","08_PAGOS_DELEGADOS")
result = result.withColumn("08_PAGOS_DELEGADOS", regexp_replace(regexp_replace("08_PAGOS_DELEGADOS", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Total S.S.","09_TOTAL_SS")
result = result.withColumn("09_TOTAL_SS", regexp_replace(regexp_replace("09_TOTAL_SS", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("PFC","10_PCF")
result = result.withColumn("10_PCF", regexp_replace(regexp_replace("10_PCF", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Bonus Año Anterior","11_Bonus_ANYO_ANTER")
result = result.withColumn("11_Bonus_ANYO_ANTER", regexp_replace(regexp_replace("11_Bonus_ANYO_ANTER", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Bonus Año Curso","12_Bonus_ANYO_CURSO")
result = result.withColumn("12_Bonus_ANYO_CURSO", regexp_replace(regexp_replace("12_Bonus_ANYO_CURSO", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Bonus Mes a Mes Contrato","13_Bmesamescontrat")
result = result.withColumn("13_Bmesamescontrat", regexp_replace(regexp_replace("13_Bmesamescontrat", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("B100% Contrato","14_B100contrato")
result = result.withColumn("14_B100contrato", regexp_replace(regexp_replace("14_B100contrato", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Bonus Mes a Mes","15_Bpagomesames")
result = result.withColumn("15_Bpagomesames", regexp_replace(regexp_replace("15_Bpagomesames", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Embargo","16_Embargo")
result = result.withColumn("16_Embargo", regexp_replace(regexp_replace("16_Embargo", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Anticipo","17_Anticipo")
result = result.withColumn("17_Anticipo", regexp_replace(regexp_replace("17_Anticipo", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Indemnización","18_Indemnizacion")
result = result.withColumn("18_Indemnizacion", regexp_replace(regexp_replace("18_Indemnizacion", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("SBA Retroactivo","19_SBA_Retroactivo")
result = result.withColumn("19_SBA_Retroactivo", regexp_replace(regexp_replace("19_SBA_Retroactivo", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Dietas","20_Dietas")
result = result.withColumn("20_Dietas", regexp_replace(regexp_replace("20_Dietas", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Referidos","21_Referidos")
result = result.withColumn("21_Referidos", regexp_replace(regexp_replace("21_Referidos", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("Incentivos","22_Incentivos")
result = result.withColumn("22_Incentivos", regexp_replace(regexp_replace("22_Incentivos", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("SeguroMedico","23_Seguro_Medico")
result = result.withColumn("23_Seguro_Medico", regexp_replace(regexp_replace("23_Seguro_Medico", ',', ''), ' ', '').cast(FloatType()))
result = result.withColumnRenamed("DNI", "NIF")
result = result.withColumnRenamed("Empleado", "NombreEmpleado")
result = result.withColumn("NIF", regexp_replace("NIF", " ", ""))

result = result.na.fill(value=0,subset=["02_IMPORTE_BRUTO", "03_DEDUCCIONES", "04_SS_OPERARIO","05_CUOTA_IRPF", "06_LIQUIDO", "07_SS_EMPRESA", "08_PAGOS_DELEGADOS","09_TOTAL_SS","10_PCF","11_Bonus_ANYO_ANTER", "12_Bonus_ANYO_CURSO", "13_Bmesamescontrat", "14_B100contrato", "15_Bpagomesames", "16_Embargo", "17_Anticipo", "18_Indemnizacion", "19_SBA_Retroactivo", "20_Dietas","21_Referidos", "23_Seguro_Medico", "NIF"])

# Group by
result = result.groupBy(['Empresa', 'NombreEmpleado', 'Categoria', 'NIF','FechaFinMes']).agg(
    sum('02_IMPORTE_BRUTO').alias('02_IMPORTE_BRUTO'),
    sum('03_DEDUCCIONES').alias('03_DEDUCCIONES'),
    sum('04_SS_OPERARIO').alias('04_SS_OPERARIO'),
    sum('05_CUOTA_IRPF').alias('05_CUOTA_IRPF'),
    sum('06_LIQUIDO').alias('06_LIQUIDO'),
    sum('07_SS_EMPRESA').alias('07_SS_EMPRESA'),
    sum('08_PAGOS_DELEGADOS').alias('08_PAGOS_DELEGADOS'),
    sum('09_TOTAL_SS').alias('09_TOTAL_SS'),
    sum('10_PCF').alias('10_PCF'),
    sum('11_Bonus_ANYO_ANTER').alias('11_Bonus_ANYO_ANTER'),
    sum('12_Bonus_ANYO_CURSO').alias('12_Bonus_ANYO_CURSO'),
    sum('13_Bmesamescontrat').alias('13_Bmesamescontrat'),
    sum('14_B100contrato').alias('14_B100contrato'),
    sum('15_Bpagomesames').alias('15_Bpagomesames'),
    sum('16_Embargo').alias('16_Embargo'),
    sum('17_Anticipo').alias('17_Anticipo'),
    sum('18_Indemnizacion').alias('18_Indemnizacion'),
    sum('19_SBA_Retroactivo').alias('19_SBA_Retroactivo'),
    sum('20_Dietas').alias('20_Dietas'),
    sum('21_Referidos').alias('21_Referidos'),
    sum('22_Incentivos').alias('22_Incentivos'),
    sum('23_Seguro_Medico').alias('23_Seguro_Medico'),
    avg('PorcentajeJornada').alias('PorcentajeJornada')
)

# # # Cambio tipo a float
result = result.withColumn("PorcentajeJornada",result.PorcentajeJornada.cast(FloatType()))
from pyspark.sql.functions import col
result = result.withColumn("02_IMPORTE_BRUTO",col("02_IMPORTE_BRUTO").cast(FloatType()))
result = result.withColumn("03_DEDUCCIONES",col("03_DEDUCCIONES").cast(FloatType()))
result = result.withColumn("04_SS_OPERARIO",col("04_SS_OPERARIO").cast(FloatType()))
result = result.withColumn("05_CUOTA_IRPF",col("05_CUOTA_IRPF").cast(FloatType()))
result = result.withColumn("06_LIQUIDO",col("06_LIQUIDO").cast(FloatType()))
result = result.withColumn("07_SS_EMPRESA",col("07_SS_EMPRESA").cast(FloatType()))
result = result.withColumn("08_PAGOS_DELEGADOS",col("08_PAGOS_DELEGADOS").cast(FloatType()))
result = result.withColumn("09_TOTAL_SS",col("09_TOTAL_SS").cast(FloatType()))
result = result.withColumn("10_PCF",col("10_PCF").cast(FloatType()))
result = result.withColumn("11_Bonus_ANYO_ANTER",col("11_Bonus_ANYO_ANTER").cast(FloatType()))
result = result.withColumn("12_Bonus_ANYO_CURSO",col("12_Bonus_ANYO_CURSO").cast(FloatType()))
result = result.withColumn("13_Bmesamescontrat",col("13_Bmesamescontrat").cast(FloatType()))
result = result.withColumn("14_B100contrato",col("14_B100contrato").cast(FloatType()))
result = result.withColumn("15_Bpagomesames",col("15_Bpagomesames").cast(FloatType()))
result = result.withColumn("16_Embargo",col("16_Embargo").cast(FloatType()))
result = result.withColumn("17_Anticipo",col("17_Anticipo").cast(FloatType()))
result = result.withColumn("18_Indemnizacion",col("18_Indemnizacion").cast(FloatType()))
result = result.withColumn("19_SBA_Retroactivo",col("19_SBA_Retroactivo").cast(FloatType()))
result = result.withColumn("20_Dietas",col("20_Dietas").cast(FloatType()))
result = result.withColumn("21_Referidos",col("21_Referidos").cast(FloatType()))
result = result.withColumn("22_Incentivos",col("22_Incentivos").cast(FloatType()))
result = result.withColumn("23_Seguro_Medico",col("23_Seguro_Medico").cast(FloatType()))

result = result.withColumn("Empresa", trim(result.Empresa))

# Sustituir nombre de la empresa por el código estandarizdo
df_Empresa = spark.sql(f"""SELECT CodEmpresaAxap, trim(EmpresaEpsilon) AS Empresa FROM {table_name_dim_empresas}""")
result = result.join(df_Empresa, 
                     (regexp_replace(df_Empresa["Empresa"], r'[^a-zA-Z0-9]', '')).contains(regexp_replace(result["Empresa"], r'[^a-zA-Z0-9]', '')), 
                     how="leftouter").select(result["*"], df_Empresa["CodEmpresaAxap"]
        )
result = result.withColumn("Empresa", result["CodEmpresaAxap"])
result = result.filter(col("Empresa").isNotNull())
result = result.drop("CodEmpresaAxap")

# Join con Bonus del Año anterior
df = spark.sql(f"""SELECT DNI, Fecha, Bonus_AA, Empresa FROM {table_name_bonus_anio_anterior}""")

result = result.join(df, [result.NIF == df.DNI, result.FechaFinMes == df.Fecha, result.Empresa == df.Empresa], how="left").select(result["*"], df["Bonus_AA"])

result = result.withColumn("Bonus_AA",result.Bonus_AA.cast(FloatType()))
result = result.na.fill(value=0, subset=["Bonus_AA"])
result = result.withColumn("ImpBrutoSinDeducciones", result["02_IMPORTE_BRUTO"]-result["03_DEDUCCIONES"]-result["18_Indemnizacion"])
result = result.withColumn("ImpSinBonusNiIncentivos", (result["02_IMPORTE_BRUTO"]-result["03_DEDUCCIONES"]-result["18_Indemnizacion"])-result["Bonus_AA"]+(-result["10_PCF"]-result["23_Seguro_Medico"])-result["22_Incentivos"])

result = result.withColumn("TotalSS", result["07_SS_EMPRESA"]-result["08_PAGOS_DELEGADOS"])

# Cambio de tipo
result = result.withColumn("ImpBrutoSinDeducciones",result.ImpBrutoSinDeducciones.cast(FloatType()))
result = result.withColumn("ImpSinBonusNiIncentivos",result.ImpSinBonusNiIncentivos.cast(FloatType()))
result = result.withColumn("TotalSS",result.TotalSS.cast(FloatType()))

# Fecha ACT
result = result.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
result = result.drop("23_Seguro_Medico")

result = result.select(col("Empresa"), col("NombreEmpleado").alias("NombreTrabajador"), col("Categoria").alias("NivelSalarial"), col("NIF"), col("FechaFinMes"), col("02_IMPORTE_BRUTO"), col("03_DEDUCCIONES"), col("04_SS_OPERARIO"), col("05_CUOTA_IRPF"), col("06_LIQUIDO"), col("07_SS_EMPRESA"), col("08_PAGOS_DELEGADOS"), col("09_TOTAL_SS"), col("10_PCF"), col("11_Bonus_ANYO_ANTER"), col("12_Bonus_ANYO_CURSO"), col("13_Bmesamescontrat"), col("14_B100contrato"), col("15_Bpagomesames"), col("16_Embargo"), col("17_Anticipo"), col("18_Indemnizacion"), col("19_SBA_Retroactivo"), col("20_Dietas"), col("21_Referidos"), col("22_Incentivos"), col("PorcentajeJornada"), col("Bonus_AA"), col("ImpBrutoSinDeducciones"), col("ImpSinBonusNiIncentivos"), col("TotalSS"), col("FECHA_ACT")).filter(col("Empresa") != 'H126')

result = result.join(df_companies, [result.Empresa == df_companies.CodEmpresaAxap], how="inner").select(result["*"])

########################
####### GUARDADO #######
########################
result.display()
condition = (col("FechaFinMes") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_epsilon, condition, result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Facturación interna ingresos

# COMMAND ----------

# DBTITLE 1,Facturación interna ingresos LOAD
##############################
####### CARGA ORIGENES #######
##############################
df_fact_interna_ingresos = spark.sql(
    f""" 
    SELECT 
        inc.IdProyecto AS DIM5, 
        inc.Concepto, 
        CAST(inc.ImpIncurrido AS FLOAT) as ImporteDecimal, 
        demp.CodEmpresaAxap as Epsilon_Empresa,
        CAST({cuentas_contables_relacion['Ingresos_Internos']} AS LONG) AS Cuenta_Contable,
        "{mes_carga} FACTURACIÓN INTERNA" AS Asiento,
        CAST('{fecha_fin_mes}' AS DATE) AS Fecha
    FROM (
        SELECT 
            i.code AS Documento,
            p.code AS IdProyecto,
            ii.concept AS Concepto,
            ROUND(ii.amount, 2) AS ImpIncurrido,
            CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
        FROM {table_name_incurrence_item} ii
        LEFT JOIN {table_name_incurrence} i ON ii.incurrence_id = i.id
        LEFT JOIN {table_name_project} p ON i.project_id = p.id
        LEFT JOIN {table_name_period} pe ON i.period_id = pe.id
        WHERE  ii.inactive = false
    ) inc
    LEFT JOIN {table_name_dim_proyecto} p ON (inc.IdProyecto = p.IdProyecto)
    LEFT JOIN {table_name_dim_cliente} c ON (p.IdCliente = c.IdCliente)
    LEFT JOIN {table_name_dim_empresas} demp ON (UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(demp.EmpresaAxapta, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'ONEINVENTORYU', '')) = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'oneinventoryu', ''), 'ISolucions', 'isolucion'), 'SistemasInformaticos', 'Sistemas')))
        WHERE (month(Fecha) = {int(mes_carga)} and year(Fecha) = {int(anyo_carga)})
        AND p.Consolidada = 1
        AND p.Empresa != 'Hiberus Sistemas Informáticos SL'
        AND (INITCAP(replace(replace(replace(replace(replace(replace(p.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o')) = INITCAP(replace(replace(replace(replace(replace(replace(replace(c.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), 'One Inventory ', '')))
        AND inc.Concepto NOT LIKE '%Refacturacion%' 
        and p.TipoActividad = 'Producción interna'
    UNION ALL
    SELECT 
        inc.IdProyecto AS DIM5, 
        inc.Concepto, 
        CAST(inc.ImpIncurrido AS FLOAT) as ImporteDecimal,
        demp.CodEmpresaAxap as Epsilon_Empresa,
        CAST({cuentas_contables_relacion['Ingresos_Internos']} AS LONG) AS Cuenta_Contable,
        "{mes_carga} FACTURACIÓN INTERNA" AS Asiento,
        CAST('{fecha_fin_mes}' AS DATE) AS Fecha
    FROM (
        SELECT 
            i.code AS Documento,
            p.code AS IdProyecto,
            ii.concept AS Concepto,
            ROUND(ii.amount, 2) AS ImpIncurrido,
            CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
        FROM {table_name_incurrence_item} ii
        LEFT JOIN {table_name_incurrence} i ON ii.incurrence_id = i.id
        LEFT JOIN {table_name_project} p ON i.project_id = p.id
        LEFT JOIN {table_name_period} pe ON i.period_id = pe.id
        WHERE  ii.inactive = false
    ) inc
    LEFT JOIN {table_name_dim_proyecto} p ON (inc.IdProyecto = p.IdProyecto)
    LEFT JOIN {table_name_dim_cliente} c ON (p.IdCliente = c.IdCliente)
    LEFT JOIN {table_name_dim_empresas} demp ON (UPPER(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(demp.EmpresaAxapta, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'ONEINVENTORYU', '')) = UPPER(REPLACE(REPLACE(REPLACE(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'oneinventoryu', ''), 'ISolucions', 'isolucion'), 'SistemasInformaticos', 'Sistemas')))
        WHERE (month(Fecha) = {int(mes_carga)} and year(Fecha) = {int(anyo_carga)})
        AND ((p.Empresa = 'Hiberus Sistemas Informáticos SL' AND c.Empresa LIKE 'Hiberus Sistemas%' ) 
        or (p.Empresa = 'Hiberus Sistemas Informáticos SL' AND c.Empresa = 'Hbu Sysop')) AND (p.TipoActividad = 'Producción interna' OR p.TipoActividad = 'Inversión') 
        AND inc.Concepto NOT LIKE '%Refacturacion%'
    """
)
    
##############################
####### TRANSFORMACIÓN #######
##############################
# Fecha ACT
result = df_fact_interna_ingresos.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
condition = (col("Fecha") >= fecha_inicio_mes) & (col("Fecha") <= fecha_fin_mes)
load_table_unity_catalog(spark, table_name_ingresos_internos, condition, result)

result.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Facturación interna gastos

# COMMAND ----------

# DBTITLE 1,Facturación interna gastos LOAD
##############################
####### CARGA ORIGENES #######
##############################
df_fact_interna_gastos = spark.sql(
    f""" 
    SELECT 
        UPPER(split(p.ParentProject4, ':')[0]) AS DIM5, 
        inc.Concepto, 
        (-1) * CAST(inc.ImpIncurrido AS FLOAT) as ImporteDecimal, 
        demp.CodEmpresaAxap as Epsilon_Empresa,
        CAST({cuentas_contables_relacion['Gastos_Internos']} AS LONG) AS Cuenta_Contable,
        "{mes_carga} FACTURACIÓN INTERNA" AS Asiento,
        CAST('{fecha_fin_mes}' AS DATE) AS Fecha
    FROM (
        SELECT 
            i.code AS Documento,
            p.code AS IdProyecto,
            ii.concept AS Concepto,
            ROUND(ii.amount, 2) AS ImpIncurrido,
            CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
        FROM {table_name_incurrence_item} ii
        LEFT JOIN {table_name_incurrence} i ON ii.incurrence_id = i.id
        LEFT JOIN {table_name_project} p ON i.project_id = p.id
        LEFT JOIN {table_name_period} pe ON i.period_id = pe.id
        WHERE  ii.inactive = false
    ) inc
    LEFT JOIN {table_name_dim_proyecto} p ON (inc.IdProyecto = p.IdProyecto)
    LEFT JOIN {table_name_dim_cliente} c ON (p.IdCliente = c.IdCliente)
    LEFT JOIN {table_name_dim_empresas} demp ON (UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(demp.EmpresaAxapta, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'ONEINVENTORYU', '')) = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'oneinventoryu', ''), 'ISolucions', 'isolucion'), 'SistemasInformaticos', 'Sistemas')))
        WHERE (month(Fecha) = {int(mes_carga)} and year(Fecha) = {int(anyo_carga)})
        AND p.Consolidada = 1
        AND p.Empresa != 'Hiberus Sistemas Informáticos SL'
        AND (INITCAP(replace(replace(replace(replace(replace(replace(p.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o')) = INITCAP(replace(replace(replace(replace(replace(replace(replace(c.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), 'One Inventory ', '')))
        AND inc.Concepto NOT LIKE '%Refacturacion%' 
        and p.TipoActividad = 'Producción interna'
    UNION
    SELECT 
        UPPER(split(p.ParentProject4, ':')[0]) AS DIM5, 
        inc.Concepto, 
        (-1) * CAST(inc.ImpIncurrido AS FLOAT) as ImporteDecimal,
        demp.CodEmpresaAxap as Epsilon_Empresa,
        CAST({cuentas_contables_relacion['Gastos_Internos']} AS LONG) AS Cuenta_Contable,
        "{mes_carga} FACTURACIÓN INTERNA" AS Asiento,
        CAST('{fecha_fin_mes}' AS DATE) AS Fecha
    FROM (
        SELECT 
            i.code AS Documento,
            p.code AS IdProyecto,
            ii.concept AS Concepto,
            ROUND(ii.amount, 2) AS ImpIncurrido,
            CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
        FROM {table_name_incurrence_item} ii
        LEFT JOIN {table_name_incurrence} i ON ii.incurrence_id = i.id
        LEFT JOIN {table_name_project} p ON i.project_id = p.id
        LEFT JOIN {table_name_period} pe ON i.period_id = pe.id
        WHERE  ii.inactive = false
    ) inc
    LEFT JOIN {table_name_dim_proyecto} p ON (inc.IdProyecto = p.IdProyecto)
    LEFT JOIN {table_name_dim_cliente} c ON (p.IdCliente = c.IdCliente)
    LEFT JOIN {table_name_dim_empresas} demp ON (UPPER(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(demp.EmpresaAxapta, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'ONEINVENTORYU', '')) = UPPER(REPLACE(REPLACE(REPLACE(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.Empresa, '.', ''), 'é', 'e'), 'á', 'a'), 'í', 'i'), 'ú', 'u'), 'ó', 'o'), ',', ''), 'SL', ''), 'Sl', ''), 'SLU', ''), ' ', ''), 'oneinventoryu', ''), 'ISolucions', 'isolucion'), 'SistemasInformaticos', 'Sistemas')))
        WHERE (month(Fecha) = {int(mes_carga)} and year(Fecha) = {int(anyo_carga)})
        AND ((p.Empresa = 'Hiberus Sistemas Informáticos SL' AND c.Empresa LIKE 'Hiberus Sistemas%' ) 
        or (p.Empresa = 'Hiberus Sistemas Informáticos SL' AND c.Empresa = 'Hbu Sysop')) AND (p.TipoActividad = 'Producción interna' OR p.TipoActividad = 'Inversión') 
        AND inc.Concepto NOT LIKE '%Refacturacion%'
    """
)
    
##############################
####### TRANSFORMACIÓN #######
##############################
# Fecha ACT
result = df_fact_interna_gastos.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
condition = (col("Fecha") >= fecha_inicio_mes) & (col("Fecha") <= fecha_fin_mes)
load_table_unity_catalog(spark, table_name_gastos_internos, condition, result)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obra en Curso Año Anterior

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
sheets = [f'FACT ANTICIPADA {anyo_carga_anterior[-2:]}', f'OBRA EN CURSO {anyo_carga_anterior[-2:]}']

xlsx_file_list = search_excel_with_company(obra_curso_anterior_mount_point, prefix_date)
df_ocaa_df = []

for (file_path, empresa) in xlsx_file_list:
    dataframe_unions = []
    for sheet in sheets:
        df = spark.read.format(file_system).option("dataAddress", f"'{sheet}'!").option("maxRowsInMemory", 25).option("header", "true").option("inferSchema", "true").load(file_path)
        df = df.withColumn("Sheet", lit(sheet))
        dataframe_unions.append(df)

    result1 = reduce(DataFrame.union, dataframe_unions)

    result1 = result1\
        .filter((col(f"ASTO {mes_carga}").isNotNull()) & (trim(col("Código")) != '') & (col(f"ASTO {mes_carga}").cast("float") != 0))\
        .select(
            when((lower(lit(empresa.split()[0])) == 'h045') & (col("Código").contains("/")), lit(cuentas_contables_relacion['Ventas_HW']))
            .otherwise(lit(cuentas_contables_relacion['Proyectos_Outsourcing_IT'])).cast(LongType()).alias("Cuenta_Contable"),
            lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
            lower(lit(empresa.split()[0])).alias("Epsilon_Empresa"),
            lit(f"OBRA EN CURSO {anyo_carga_anterior}").alias("Asiento"),
            regexp_replace(col(f"ASTO {mes_carga}"), ",", "").cast(FloatType()).alias("Importe_Decimal"),
            col("Sheet").alias("Concepto"),
            col("Código").alias("DIM5")
        )
    df_ocaa_df.append(result1)

df_ocaa = reduce(DataFrame.union, df_ocaa_df)

##############################
####### TRANSFORMACIÓN #######
##############################
df_ocaa = df_ocaa.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))  

########################
####### GUARDADO #######
########################
df_ocaa.display()

condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_obra_curso_anio_anterior, condition, df_ocaa)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obra en Curso Año Actual

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
xlsx_file_list = search_excel_with_company(oc_mount_point, prefix_date)
list_result = []

for file_path, empresa in xlsx_file_list:
    df = spark.read.format(file_system).option("dataAddress", f"'ObraEnCurso'!").option("maxRowsInMemory", 25).option("header", "true").load(file_path) 
    for col_name in df.columns:
        new_col_name = col_name.replace(' ', '').replace('(', '').replace(')', '').replace('ó', 'o').replace('-', '')
        df = df.withColumnRenamed(col_name, new_col_name)

    df = df.fillna({'Tipoproyecto': ''})
    df = df.fillna('') 

    result_select = df \
        .filter((lower(col("Tipoactividad")) == "cliente final") & (col("TRANSACCIONES").cast(FloatType()) != 0))\
        .select(
            col("Codigo"),
            col("Cliente"),
            col("Proyecto"),
            col("Tipoactividad"),
            col("Tipoproyecto"),
            col("Estado"),
            col("Empresa"),
            col("TRANSACCIONES").cast(DoubleType()).alias("TRANSACCIONES"),
            lit(f"TRANSACCIONES{mes_carga}").alias("TRANSACCIONES_T"),
            when((col("TRANSACCIONES").cast(DoubleType()) > 0), 9705720)\
                .when((col("TRANSACCIONES").cast(DoubleType()) <= 0) , 9705721).otherwise(0).cast(IntegerType()).alias("CuentaContable"),
            lower(lit(empresa.split()[0])).alias("Cod_Empresa"),
            lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
            lit(f"OC {anyo_carga}").alias("Concepto")
            )

    list_result.append(result_select)

result = reduce(DataFrame.union, list_result)

##############################
####### TRANSFORMACIÓN #######
##############################
result = result.withColumn('Grupo_NoGrupo', lit(""))

result = result.withColumn(fecha_act_str, date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))
result = result.dropDuplicates()

########################
####### GUARDADO #######
########################
condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_obra_curso, condition, result)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obra en Curso Año Actual COMPENSADO

# COMMAND ----------

##############################
####### CARGA ORIGENES #######
##############################
df = spark.sql(f"""SELECT Codigo AS CodigoProyecto, TRANSACCIONES, TRANSACCIONES_T, CuentaContable, Grupo_NoGrupo, Cod_Empresa, Concepto,Fecha FROM {table_name_obra_curso}""")

##############################
####### TRANSFORMACIÓN #######
##############################
# Filtro
df = df.filter( (col("Fecha") == fecha_fin_mes) | (col("Fecha") == fecha_fin_mes_prev) & (col("Concepto") == f'OC {anyo_carga}') )
df = df.withColumn("TRANSACCIONES", when(col("TRANSACCIONES_T") == f"TRANSACCIONES{mes_prev_string}", -1 * col("TRANSACCIONES")).otherwise(col("TRANSACCIONES")))
rt = df.groupBy("CodigoProyecto", "Cod_Empresa", "Concepto").agg(sum("TRANSACCIONES").alias("TRANSACCIONES"))
rt = rt.sort("CodigoProyecto")
rt = rt.filter(col("TRANSACCIONES") != 0)
rt = rt.select(
    when((col("TRANSACCIONES") > 0) & (col("Cod_Empresa") != 'h045'), lit(cuentas_contables_relacion['Obra_En_Curso']))
    .when((col("TRANSACCIONES") <= 0) & (col("Cod_Empresa") != 'h045'), lit(cuentas_contables_relacion['Facturacion_Anticipada']))
    .when((col("TRANSACCIONES") > 0) & (col("Cod_Empresa") == 'h045') & (~col("CodigoProyecto").contains("/")), lit(cuentas_contables_relacion['Obra_En_Curso']))
    .when((col("TRANSACCIONES") <= 0) & (col("Cod_Empresa") == 'h045') & (~col("CodigoProyecto").contains("/")), lit(cuentas_contables_relacion['Facturacion_Anticipada']))
    .otherwise(lit(cuentas_contables_relacion['Obra_En_Curso_Ventas_HW'])).cast(LongType()).alias("Cuenta_Contable"),
    lit(fecha_fin_mes).cast(DateType()).alias("Fecha"),
    lower(col("Cod_Empresa")).alias("Epsilon_Empresa"),
    concat(lit(mes_carga),lit(" "), col("Concepto")).alias("Asiento"),
    col("TRANSACCIONES").cast(FloatType()).alias("Importe_Decimal"),
    col("Concepto"),
    col("CodigoProyecto").alias("DIM5")
)

# Fecha
rt = rt.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

########################
####### GUARDADO #######
########################
condition = (col("Fecha") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_obra_anio_actual, condition, rt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incentivos

# COMMAND ----------

# DBTITLE 1,Incentivos LOAD
##############################
####### CARGA ORIGENES #######
##############################
df_incentivos = spark.read.format(file_system).option("header", "true").load(spark_path_incentivos)
#.option("dataAddress", f"'INCENTIVOS'!")
df_companies = spark.sql(f"SELECT CodEmpresaAxap FROM {table_name_dim_empresas} WHERE CodEmpresaAxap != 'H126'")

##############################
####### TRANSFORMACIÓN #######
##############################
incentivos_final = df_incentivos.select(
    lit(fecha_fin_mes).cast(DateType()).alias("FechaCargaFin"), 
    trim(col("UsuarioNIF")).alias("DNI"),
    col("CodigoProyecto").alias("DIM5"), 
    lower(col("Código empresa")).alias("Empresa"), 
    lit(None).alias("NivelSalarial"),
    regexp_replace(col("Incentivo"), ",", "").cast(FloatType()).alias("Importe_Decimal"),    
    col("Cuenta contable").alias("Cuenta_Contable"),
    upper(col("Usuario")).alias("NOMBRE"),
    lit(None).alias("Asiento_OC")
)
#incentivos_final = incentivos_final.dropDuplicates()
incentivos_final = incentivos_final.join(df_companies, [incentivos_final.Empresa == df_companies.CodEmpresaAxap], how="inner").select(incentivos_final["*"])
 
########################
####### GUARDADO #######
########################
condition = (col("FechaCargaFin") == fecha_fin_mes)
load_table_unity_catalog(spark, table_name_incentivos, condition, incentivos_final)

incentivos_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TABLAS CARGA DIMENSIONES

# COMMAND ----------

# MAGIC %md
# MAGIC **Asientos** Relación de asientos contables de cada mes con su cuenta contable.

# COMMAND ----------

if carga_asientos == "Y":
  # Constants
  rows_to_start = "A4"

  # Transformation
  result = spark.read.format(file_system).option(header_str, "true").option(infer_sch_str, "true").option(data_add_str, f"'{asientos_sheet}'!{rows_to_start}").load(path_spark_asientos)

  result = result.withColumnRenamed("CUENTA CONTABLE", cuenta_contable)
  result = result.withColumnRenamed("ASIENTO", asiento_oc)

  result = result.withColumnRenamed('FECHA', fecha_str)
  result = result.withColumn(fecha_str, to_date(fecha_str, date_format_bar))

  result = result.filter( col(fecha_str) == fecha_fin_mes)

  condition = (col(fecha_str) == fecha_fin_mes)
  load_table_unity_catalog(spark, table_name_asientos, condition, result)

  result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dimension Empresas**
# MAGIC Relación entre el código de la empresa Hiberus y su descripción.

# COMMAND ----------

if carga_dim_empresa == 'N':
    result = spark.read.format(file_system).option(header_str, "true").load(path_spark_dim_empresas)

    result = result.dropDuplicates()

    result = result.select(
             when(col("CodEmpresaAxap") == 'h045', 11) \
            .when(col("CodEmpresaAxap") == 'h051', 4) \
            .when(col("CodEmpresaAxap") == 'h044', 7) \
            .when(col("CodEmpresaAxap") == 'h083', 5) \
            .when(col("CodEmpresaAxap") == 'h057', 12) \
            .when(col("CodEmpresaAxap") == 'h069', 9) \
            .when(col("CodEmpresaAxap") == 'h077', 13) \
            .when(col("CodEmpresaAxap") == 'h095', 8) \
            .when(col("CodEmpresaAxap") == 'h094', 1) \
            .when(col("CodEmpresaAxap") == 'h092', 14) \
            .when(col("CodEmpresaAxap") == 'h093', 2) \
            .when(col("CodEmpresaAxap") == 'h102', 15) \
            .when(col("CodEmpresaAxap") == 'h053', 10) \
            .when(col("CodEmpresaAxap") == 'h049', 3) \
            .when(col("CodEmpresaAxap") == 'H126', 22) \
            .otherwise(0).alias("IdEmpresa"),
             when(col("CodEmpresaAxap") == 'h045', 'Sistemas') \
            .when(col("CodEmpresaAxap") == 'h051', 'Semantica') \
            .when(col("CodEmpresaAxap") == 'h044', 'Hiberus') \
            .when(col("CodEmpresaAxap") == 'h083', 'Media') \
            .when(col("CodEmpresaAxap") == 'h057', 'Osaba') \
            .when(col("CodEmpresaAxap") == 'h069', 'Travel') \
            .when(col("CodEmpresaAxap") == 'h077', 'Serveis') \
            .when(col("CodEmpresaAxap") == 'h095', 'Diferenciales') \
            .when(col("CodEmpresaAxap") == 'h094', 'Solutions') \
            .when(col("CodEmpresaAxap") == 'h092', 'IT') \
            .when(col("CodEmpresaAxap") == 'h093', 'Digital') \
            .when(col("CodEmpresaAxap") == 'h102', 'IKT') \
            .when(col("CodEmpresaAxap") == 'h053', 'Ovvoe') \
            .when(col("CodEmpresaAxap") == 'h049', 'Search') \
            .when(col("CodEmpresaAxap") == 'H126', 'Trackglobe') \
            .otherwise(0).alias("EmpresaABREV"),
            when(((col("CodEmpresaAxap") == 'h057') | (col("CodEmpresaAxap") == 'h077') | (col("CodEmpresaAxap") == 'h102')), 'Filiales territoriales') \
            .when(((col("CodEmpresaAxap") == 'h069') | (col("CodEmpresaAxap") == 'h083') | (col("CodEmpresaAxap") == 'h051')), 'Filiales tecnológicas') \
            .otherwise(col('EmpresaAxapta')).alias("AgrupacionFiliales"),
            when(((col("CodEmpresaAxap") == 'h093') | (col("CodEmpresaAxap") == 'h095') | (col("CodEmpresaAxap") == 'h044') | (col("CodEmpresaAxap") == 'h045') | (col("CodEmpresaAxap") == 'h094') | (col("CodEmpresaAxap") == 'h092')), 'CORE') \
            .otherwise('Otras').alias("AgrupacionEmpresas"),
        col("CodEmpresaAxap"),
        col("EmpresaAxapta"),
        col("EmpresaUnh"),
        col("EmpresaEpsilon")
    )
    result.write.mode("overwrite").format("delta").saveAsTable(table_name_dim_empresas)

    result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Proyectos vacaciones y genéricas**

# COMMAND ----------

# DBTITLE 1,Proyectos vacaciones y genéricas LOAD
if carga_tablas == 'Y':
  df_vacaciones = spark.read.format(file_system).option("header", "true").load(path_spark_vacaciones)
  traduccionProyectos = df_vacaciones.filter(col("GENÉRICA").isNotNull()).select(col("Proyecto Vacaciones").alias("DIM5"), col("GENÉRICA").alias("Cuenta_generica"))
  traduccionProyectos.write.mode("overwrite").format("delta").saveAsTable(table_name_vacaciones)

  traduccionProyectos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Cuentas Contables**
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cuentas Contrables LOAD
if carga_tablas == 'Y':
    result = spark.read.format(file_system).option("header", "true").load(path_spark_cuentas_contables)

    columns = result.columns
    columns_renamed = [column.replace(".", "_").replace("0", "").replace("2", "") for column in columns]

    result = result.withColumnRenamed("Cuenta contable0", "Cuenta_contable")
    result = result.withColumnRenamed("Nombre de la cuenta", "Nombre_cuenta")
    result = result.withColumnRenamed("Cuenta contable2", "Cuenta_contable_1")
    result = result.withColumnRenamed("Epígrafe", "Epigrafe")
    result = result.withColumnRenamed("Subepígrafe", "Subepigrafe")
    result = result.withColumnRenamed("I/G", "IG")

    result = result.select(
        col("Cuenta_contable").cast(IntegerType()).alias("IdCuenta"),
        col("Cuenta_contable_1").alias("CuentaContable"),
        rtrim(ltrim("IG")).alias("IG"),
        col("Grupo"),
        col("Epigrafe"),
        col("Subepigrafe"),
        when((rtrim(ltrim("IG")) == 'Ingresos'), 0)\
        .when((rtrim(ltrim("IG")) == 'Gastos'), 1)\
        .when((rtrim(ltrim("IG")) == 'Resultados extraordinarios'), 2)\
        .when((rtrim(ltrim("IG")) == 'Resultado de deterioros y enajenaciones inmov.'), 3)\
        .when((rtrim(ltrim("IG")) == 'Pérdidas, deterioros y variac. Provisiones comerciales'), 4)\
        .when((rtrim(ltrim("IG")) == 'Amortización'), 5)\
        .when((rtrim(ltrim("IG")) == 'Resultados Financieros'), 6)\
        .otherwise(None).alias('Orden_IG')
    )
    result = result.dropDuplicates()
    result = result.withColumn("Orden_IG_int", lpad(col("Orden_IG"), 2, "0"))

    result = result.withColumn("NivelIG", concat(col('Orden_IG_int'), lit('000000')))

    window_check = Window.partitionBy("Grupo").orderBy("Grupo")
    result = result.withColumn("row_num_grupo", row_number().over(window_check))
    window_min = Window.partitionBy("Grupo")
    result = result.withColumn("min_row_num_grupo", min("row_num_grupo").over(window_min))
    windowSpec = Window.orderBy(col("min_row_num_grupo"), col("Grupo"))
    result = result.withColumn("OrdenGR_int", dense_rank().over(windowSpec))
    result = result.withColumn("OrdenGR_int", lpad(when(col("Grupo").isNull(), None).otherwise(col("OrdenGR_int").cast("string")), 2, "0"))
    result = result.withColumn("NivelGR", concat(col('Orden_IG_int'), col('OrdenGR_int'), lit('0000')))

    window_check = Window.partitionBy("Epigrafe").orderBy("Epigrafe")
    result = result.withColumn("row_num_epigrafe", row_number().over(window_check))
    window_min = Window.partitionBy("Epigrafe")
    result = result.withColumn("min_row_num_epigrafe", min("row_num_epigrafe").over(window_min))
    windowSpec = Window.orderBy(col("min_row_num_epigrafe"), col("Epigrafe"))
    result = result.withColumn("OrdenEP_int", dense_rank().over(windowSpec))
    result = result.withColumn("OrdenEP_int", lpad(when(col("Epigrafe").isNull(), None).otherwise(col("OrdenEP_int").cast("string")), 2, "0"))
    result = result.withColumn("NivelEP", concat(col('Orden_IG_int'), col('OrdenGR_int'), col('OrdenEP_int'), lit('00')))

    window_check = Window.partitionBy("Subepigrafe").orderBy("Subepigrafe")
    result = result.withColumn("row_num_subepigrafe", row_number().over(window_check))
    window_min = Window.partitionBy("Subepigrafe")
    result = result.withColumn("min_row_num_subepigrafe", min("row_num_subepigrafe").over(window_min))
    windowSpec = Window.orderBy(col("min_row_num_subepigrafe"), col("Subepigrafe"))
    result = result.withColumn("OrdenSE_int", dense_rank().over(windowSpec))
    result = result.withColumn("OrdenSE_int", lpad(when(col("Subepigrafe").isNull(), None).otherwise(col("OrdenSE_int").cast("string")), 2, "0"))
    result = result.withColumn("NivelSE", concat(col('Orden_IG_int'), col('OrdenGR_int'), col('OrdenEP_int'), col('OrdenSE_int')))

    result = result.drop("Orden_IG_int")
    result = result.drop("row_num_grupo", "min_row_num_grupo", "OrdenGR_int")
    result = result.drop("row_num_epigrafe", "min_row_num_epigrafe", "OrdenEP_int")
    result = result.drop("row_num_subepigrafe", "min_row_num_subepigrafe", "OrdenSE_int")

    # Fecha ACT
    result.write.mode("overwrite").format("delta").saveAsTable(table_name_cuentas_contables)

    result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Empresas Interco**

# COMMAND ----------

# DBTITLE 1,Empresas Interco LOAD
if carga_tablas == 'Y':
  result = spark.read.format(file_system).option("header", "true").load(path_spark_empresas_interco)

  result = result.dropDuplicates()
  result = result.withColumnRenamed("AX/UNH","AX_UNH")
  result = result.withColumnRenamed("Cliente interco","Cliente_Interco")
  result = result.select("Cliente_Interco","Interco","AX_UNH")

  # Fecha ACT
  result = result.withColumn("FECHA_ACT", date_format(current_timestamp(), "yyyy-MM-dd' 'HH:mm:ss"))

  print('Realizando el insert en la tabla de Empresas_Interco')
  result.write.mode("overwrite").format("delta").saveAsTable(table_name_empresas_interco)

  result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histórico (año en curso-mes anterior)

# COMMAND ----------

# DBTITLE 1,Carga histórico año anterior
if carga_datos_historicos == "Y":
    sheet_name = get_column_names(mount_point_historico_transacciones)[0] # preventive
    result_current_year = spark.read.format(file_system).option("header", "true").option("inferSchema", "true").option("dataAddress", f"'{sheet_name}'!").option("maxRowsInMemory", 25).load(spark_path_historico_transacciones)

    df = result_current_year.withColumnRenamed("AÑO","Anyo")
    df = df.withColumn("NombreMes", col("Mes"))
    df = df.withColumnRenamed("Cliente Unhiberse","Cliente_Unhiberse")
    df = df.withColumnRenamed("Proyecto Unhiberse","Proyecto_Unhiberse")
    df = df.withColumnRenamed("Grupo CLIENTE UNHIBERSE","Grupo_CLIENTE_UNHIBERSE")
    df = df.withColumnRenamed("Cliente / Proveedor","Cliente_Proveedor")
    df = df.withColumnRenamed("Nombre de la cuenta","Cuenta_contable_1")
    df = df.withColumnRenamed("SubHBU","DIM_6")
    df = df.withColumnRenamed("Mercado","DIM_7")
    df = df.withColumnRenamed("Año/mes","Tipo_proyecto")
    df = df.withColumnRenamed("Resultados extraordinarios","ResultadosExtraordinarios")
    df = df.withColumnRenamed("Resultado de deterioros y enajenaciones inmov.","ResultadosDeterEnajInmov")
    df = df.withColumnRenamed("Amortización","Amortizacion")
    df = df.withColumnRenamed("Pérdidas, deterioros y variac. Provisiones comerciales","PerdidasDetVarProvComerciales")
    df = df.withColumnRenamed("Resultados Financieros","ResultadosFinancieros")
    df = df.withColumnRenamed("Nombre empresa","Nombre_empresa")
    df = df.withColumnRenamed("Nombre cuenta","NombreCuenta")
    df = df.withColumnRenamed("Nombre proveedor","NombreProveedor")
    df = df.withColumnRenamed("Nombre cliente","NombreCliente")
    df = df.withColumnRenamed("Código proyecto cliente","C_digo_proyecto_cliente")
    df = df.withColumnRenamed("Mercado / SMB","Mercado_SMB")
    df = df.withColumnRenamed("HMA PRIMER NIVEL","HMA_PRIMER_NIVEL")
    df = df.withColumnRenamed("Interco financiero","Interco")
    df = df.withColumnRenamed("Interco analítico (con OC)","Interco_anal_tico_con_OC")
    df = df.withColumnRenamed("Gastos delegaciones","Gastos_delegaciones")
    df = df.withColumnRenamed("Proyectos PPISA","Proyectos_PPISA")
    df = df.withColumnRenamed("Tipo ","IG")
    df = df.withColumnRenamed("Empresa","NombreEmpresa")
    df = df.withColumnRenamed("Importe","ImporteDecimal")
    df = df.withColumnRenamed("Codigo_Proyecto","Varios")
    df = df.withColumnRenamed("Tipo proyecto","Proyecto")
    df = df.withColumnRenamed("Fuente maestro PYG", "Tipo")

    df = df.replace(to_replace=months, subset=['NombreMes'])
    df = df.withColumn("FECHA_ACT", current_timestamp())

    df = df.drop("HTU", "HMA", "HBU", "DIM_6", "DIM_7", "HGC", "Proyectos_PPISA", "Gastos_delegaciones", "Interco_anal_tico_con_OC", "HMA_PRIMER_NIVEL", "Nombre_empresa", "Area", "Medio", "Submedio", "Producto", "DIM6", "DIM7", "Cliente_Unhiberse", "Proyecto_Unhiberse", "Grupo_CLIENTE_UNHIBERSE", "Cliente_Proveedor",   "C_digo_proyecto_cliente", "Mercado_SMB", "Proyecto", "Tipo_proyecto", "Cuenta de contable", "Foco", "País", "Coste de estructura", "Datos", )

    df = df.withColumn("Anyo", df.Anyo.cast(IntegerType()))
    df = df.withColumn("Cuenta", df.Cuenta.cast(IntegerType()))
    df = df.withColumn("Fecha", df.Fecha.cast(IntegerType()))
    df = df.withColumn("Fecha", expr("cast(date_add('1899-12-31', Fecha - 1) as date)"))
    df = df.withColumn("ImporteDecimal", df.ImporteDecimal.cast(FloatType()))
    df = df.withColumn("ResultadosExtraordinarios", df.ResultadosExtraordinarios.cast(FloatType()))
    df = df.withColumn("ResultadosDeterEnajInmov", df.ResultadosDeterEnajInmov.cast(FloatType()))
    df = df.withColumn("EBITDA", df.EBITDA.cast(FloatType()))
    df = df.withColumn("Ingresos", df.Ingresos.cast(FloatType()))
    df = df.withColumn("Gastos", df.Gastos.cast(FloatType()))
    df = df.withColumn("Amortizacion", df.Amortizacion.cast(FloatType()))
    df = df.withColumn("PerdidasDetVarProvComerciales", df.PerdidasDetVarProvComerciales.cast(FloatType()))
    df = df.withColumn("ResultadosFinancieros", df.ResultadosFinancieros.cast(FloatType()))
    df = df.withColumn("EBT", df.EBT.cast(FloatType()))
    df = df.withColumn("Mes", month(col("Fecha")))
    df = df.withColumnRenamed("Importe financiero", "ImporteFinancieroVDisfrutadas")

    df_hist = df.withColumn('Orden_IG',
            when((col("IG") == 'Ingresos'), 0)\
            .when((col("IG") == 'Gastos'), 1)\
            .when((col("IG") == 'Resultados extraordinarios'), 2)\
            .when((col("IG") == 'Resultado de deterioros y enajenaciones inmov.'), 3)\
            .when((col("IG") == 'Pérdidas, deterioros y variac. Provisiones comerciales'), 4)\
            .when((col("IG") == 'Amortización'), 5)\
            .when((col("IG") == 'Resultados Financieros'), 6)\
            .otherwise(7)
        )
    df_hist.write.mode("overwrite").format("delta").saveAsTable(table_name_transacciones_historico)