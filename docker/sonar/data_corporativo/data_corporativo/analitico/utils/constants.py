# Databricks notebook source
# DBTITLE 1,Constants
header_str = "header"
infer_sch_str = "inferSchema"
data_add_str = "dataAddress"
fecha_str = "Fecha"
fecha_act_str = "FECHA_ACT"
empresa_str = "Empresa"
cuenta_contable = "Cuenta_Contable"
asiento_oc = "Asiento_OC"
date_format_bar = "dd/MM/yyyy"
timestamp_act_format = "yyyy-MM-dd' 'HH:mm:ss"
asientos_sheet = "ASIENTOS"

# COMMAND ----------

# DBTITLE 1,table variables
# MODIFY THIS PARAMETER MANUALLY
N_PREVIOUS_MONTHS = -2
mounting_point = '/Volumes/control/bronze_analitico/files_analitico/'
file_system = "com.crealytics.spark.excel"
SHAREPOINT_SITE = "https://hiberus.sharepoint.com/sites/PowerBI"
SHAREPOINT_SITE_NAME = "PowerBI"
SHAREPOINT_DOC = "Documentos compartidos/"

# Dictionary of months
months_dict = {
    "Enero" : "01",
    "Febrero" : "02",
    "Marzo" : "03",
    "Abril" : "04",
    "Mayo" : "05",
    "Junio" : "06",
    "Julio" : "07",
    "Agosto" : "08",
    "Septiembre" : "09",
    "Octubre" : "10",
    "Noviembre" : "11",
    "Diciembre" : "12"
}

meses_dict = {
    "01": "Enero",
    "02": "Febrero",
    "03": "Marzo",
    "04": "Abril",
    "05": "Mayo",
    "06": "Junio",
    "07": "Julio",
    "08": "Agosto",
    "09": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre",
}


# COMMAND ----------

# DBTITLE 1,Cuentas contables
cuentas_contables_relacion = {
    'Sueldos_Salarios_Directo': '9640100',
    'Vacaciones_Disfrutadas_Directo': '9640101',
    'Seguridad_Social_Directo': '9642100',
    'Vacaciones_Disfrutadas_SS_Directo': '9642101',
    'Anulacion_Bonus': '9640998',
    'Sueldos_Salarios_Directo_640': '6400000',
    'Retribuciones_En_Especie': '6400005',
    'Sueldos_Y_Salarios_Interno': '9640000',
    'Seguridad_Social_A_Cargo_De_La_Empresa': '6420000',
    'Seguridad_Social_A_Cargo_De_La_Empresa_Interno': '9642000',
    'Becarios_Productivos': '6294002',
    'Becarios_Productivos_962': '9629402',
    'Periodificacion_Directos': '6400998',
    'Periodificacion_Indirectos': '6400999',
    'Incentivos': '9640007',
    'Ingresos_Internos': '9705000',
    'Gastos_Internos': '9607001',
    'Ventas_HW': '7001710',
    'Proyectos_Outsourcing_IT': '7057200',
    'Obra_En_Curso': '9705720',
    'Facturacion_Anticipada': '9705721',
    'Obra_En_Curso_Ventas_HW': '9700171',
    'Periodificacion_Bonus_Dimensiones': '9640103',
}

# COMMAND ----------

# DBTITLE 1,Table names
schema_name_bronze = "control.bronze_analitico"
schema_name_silver = "control.silver_analitico"
schema_name_gold = "control.gold_analitico"
sufix_tables_catalog = ""

# BRONZE TABLES

#TODO: Substitute to original fact_incurridos table
table_name_period = "operaciones.silver_unhiberse.period"
table_name_incurrence_item = "operaciones.silver_unhiberse.incurrence_item"
table_name_incurrence = "operaciones.silver_unhiberse.incurrence"
table_name_project = "operaciones.silver_unhiberse.project"

table_name_dim_proyecto = "operaciones.gold_unhiberse.dim_proyecto"
table_name_dim_cliente = "operaciones.gold_unhiberse.dim_cliente"

table_name_asientos = f"{schema_name_bronze}.Asientos{sufix_tables_catalog}"
table_name_axapta = f"{schema_name_bronze}.Axapta{sufix_tables_catalog}"
table_name_bonus_anyo_actual = f"{schema_name_bronze}.Bonus_Anyo_Actual{sufix_tables_catalog}"
table_name_horas_kvp = f"{schema_name_bronze}.Horas_KVP{sufix_tables_catalog}"
table_name_bonus_anio_anterior = f'{schema_name_bronze}.Bonus_Anyo_Anterior{sufix_tables_catalog}'
table_name_epsilon = f"{schema_name_bronze}.Epsilon{sufix_tables_catalog}"
table_name_ingresos_internos = f"{schema_name_bronze}.Ingresos_Internos{sufix_tables_catalog}"
table_name_gastos_internos = f"{schema_name_bronze}.Gastos_Internos{sufix_tables_catalog}"
table_name_obra_curso = f"{schema_name_bronze}.Obra_Actual{sufix_tables_catalog}"
table_name_obra_anio_actual = f"{schema_name_bronze}.Obras_Anyo_Actual{sufix_tables_catalog}"
table_name_incentivos = f"{schema_name_bronze}.Incentivos{sufix_tables_catalog}"
table_name_vacaciones = f"{schema_name_bronze}.Vacaciones{sufix_tables_catalog}"
table_name_empresas_interco = f"{schema_name_bronze}.Empresas_Interco{sufix_tables_catalog}"
table_name_obra_curso_anio_anterior = f"{schema_name_bronze}.Obras_Anyo_Anterior{sufix_tables_catalog}"
table_name_transacciones_historico = f"{schema_name_bronze}.Transacciones_Historico{sufix_tables_catalog}"
table_ajuste_vacaciones = f"{schema_name_bronze}.empresas_ajuste_vacaciones"

# SILVER TABLES
table_name_Epsilon_KVP = f"{schema_name_silver}.Epsilon_KVP{sufix_tables_catalog}"
table_name_asto_01 = f"{schema_name_silver}.ASTO_01{sufix_tables_catalog}"
table_name_asto_02 = f"{schema_name_silver}.ASTO_02{sufix_tables_catalog}"
table_name_asto_03 = f"{schema_name_silver}.ASTO_03{sufix_tables_catalog}"
table_name_asto_negativo_01 = f"{schema_name_silver}.ASTO_N01{sufix_tables_catalog}"
table_name_asto_negativo_02 = f"{schema_name_silver}.ASTO_N02{sufix_tables_catalog}"
table_name_asto_negativo_03 = f"{schema_name_silver}.ASTO_N03{sufix_tables_catalog}"
tabla_name_becarios = f"{schema_name_silver}.ASTO_Becarios{sufix_tables_catalog}"
tabla_name_negativos_becarios = f"{schema_name_silver}.ASTO_NBecarios{sufix_tables_catalog}"
tabla_name_final = f"{schema_name_silver}.transacciones{sufix_tables_catalog}"

# GOLD TABLES
tabla_final_modeled = f"{schema_name_gold}.fact_transacciones{sufix_tables_catalog}"
tabla_proveedores_modeled = f"{schema_name_gold}.dim_proveedores{sufix_tables_catalog}"
tabla_clientes_modeled = f"{schema_name_gold}.dim_clientes{sufix_tables_catalog}"
table_name_cuentas_contables = f"{schema_name_gold}.dim_analitica{sufix_tables_catalog}"
table_name_dim_empresas = f"{schema_name_gold}.Dim_Empresas{sufix_tables_catalog}"