# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS plataformas.silver_crm")

# COMMAND ----------

# Read the table from the specified path
df = spark.table("plataformas.bronze_crm.lista_proyectos")


# Display original schema
print("Original Schema:")
df.printSchema()

# Get the schema as a dictionary to identify array columns
schema_dict = {field.name: field.dataType for field in df.schema.fields}

# Find all array columns
array_columns = [col_name for col_name, data_type in schema_dict.items() 
                if isinstance(data_type, ArrayType)]

print(f"\nDetected {len(array_columns)} array columns: {array_columns}")

# First approach: Convert arrays to strings using array_join
# This works well for simple arrays of primitives
def convert_simple_arrays(df, array_columns):
    result_df = df
    for col_name in array_columns:
        print(f"Converting simple array column: {col_name}")
        result_df = result_df.withColumn(
            col_name, 
            F.when(
                F.col(col_name).isNotNull(),
                F.array_join(F.col(col_name), ", ")
            ).otherwise(None)
        )
    return result_df

# Second approach: For complex arrays (arrays of structs/maps)
# Process each array column and convert to string representation
def process_complex_arrays(df, array_columns):
    result_df = df
    
    for col_name in array_columns:
        print(f"Processing complex array column: {col_name}")
        
        # Check if this is an array of structs/maps (complex type)
        sample_col = result_df.select(col_name).first()
        if sample_col and sample_col[0] and isinstance(sample_col[0], (list, dict)):
            print(f"  - Complex array detected in {col_name}")
            
            # Create a temp view with exploded array
            temp_view_name = f"temp_view_{col_name}"
            exploded_df = result_df.select("*", F.explode(col_name).alias(f"{col_name}_item"))
            
            # Convert the exploded items to JSON strings
            exploded_df = exploded_df.withColumn(f"{col_name}_item_str", F.to_json(F.col(f"{col_name}_item")))
            exploded_df.createOrReplaceTempView(temp_view_name)
            
            # Group by all columns except the exploded one and collect the strings as an array
            columns_to_group = [c for c in exploded_df.columns 
                              if c not in [col_name, f"{col_name}_item", f"{col_name}_item_str"]]
            
            # Use SQL for cleaner aggregation
            grouped_df = spark.sql(f"""
                SELECT 
                    {', '.join(columns_to_group)},
                    CONCAT('[', CONCAT_WS(', ', COLLECT_LIST({col_name}_item_str)), ']') as {col_name}
                FROM {temp_view_name}
                GROUP BY {', '.join(columns_to_group)}
            """)
            
            result_df = grouped_df
        else:
            # For simple arrays, use array_join
            result_df = result_df.withColumn(
                col_name, 
                F.when(
                    F.col(col_name).isNotNull(),
                    F.array_join(F.col(col_name), ", ")
                ).otherwise(None)
            )
    
    return result_df

# Try to determine if we have simple or complex arrays
# Sample first row of first array column to check type
try:
    if array_columns:
        sample = df.select(array_columns[0]).first()
        if sample and sample[0]:
            first_element = sample[0][0] if len(sample[0]) > 0 else None
            
            if first_element and isinstance(first_element, (dict, list)):
                print("\nDetected complex array elements (structs/maps). Using complex conversion approach.")
                df_converted = process_complex_arrays(df, array_columns)
            else:
                print("\nDetected simple array elements. Using array_join approach.")
                df_converted = convert_simple_arrays(df, array_columns)
        else:
            print("\nEmpty arrays detected. Using simple conversion approach.")
            df_converted = convert_simple_arrays(df, array_columns)
    else:
        print("\nNo array columns found.")
        df_converted = df
except Exception as e:
    print(f"\nError analyzing array types: {str(e)}")
    print("Falling back to simple array conversion.")
    df_converted = convert_simple_arrays(df, array_columns)

# Display sample of converted data
if array_columns:
    print("\nSample data with converted columns:")
    df_converted.select("*").limit(10).display()
    
    # Display new schema
    print("\nNew Schema (with string columns):")
    df_converted.printSchema()
    
    # Save the converted DataFrame if needed
    # df_converted.write.format("delta").mode("overwrite").saveAsTable("operaciones.bronze_crm.lista_proyectos_converted")
    
    print("\nConversion complete. Array columns have been converted to string format.")
else:
    print("\nNo array columns found in the table.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtro fechas

# COMMAND ----------

df_converted.count()

# COMMAND ----------

# DBTITLE 1,Filtro 2024

df_converted_2024 = df_converted[df_converted["FechaPresentacion"] >= '2024-01-01'] # Importante fecha de presentacion

# COMMAND ----------

df_converted_2024.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact y dim tables
# MAGIC y sus respectivas transformaciones

# COMMAND ----------

df_converted_2024.columns
# capacidaes tenemos en gold: id (IdCapacidad) (unica forma que hay de poder traerse el fabricante), Area, SubArea, titulo, cliente, sector, fecha, zonageo, fechacarga
# nos vamos a quedar con: IdProy, oportunidad, ofderta, probabilidad, duracion (silver pero no gold), coste (silver pero no gold), presupuesto (TCV), Importe licitado (silver pero no gold), margen (silver pero no gold), fechapresenatcion (es la fecha de filtrar enero 2024), tipologia, título, cliente (podriamos leerlo de unhiberse asi que el que viene nos lo cargariamos y lo cambiariamos por el de unhib) , importe, estado oportunidades (es como estado peero ya agrupado) (muy importante)

# COMMAND ----------

df_converted_2024 = df_converted_2024.withColumn("FechaCarga", F.current_timestamp())

# COMMAND ----------

df_converted_2024.distinct().count()

# COMMAND ----------

# DBTITLE 1,Formato columnas oportunidades
Fact_CRM_df_oportunidades = df_converted_2024.select(
    F.col("IdProyecto").alias("IdProyecto"), # si 
    #F.col("MotivoEstado").alias("MotivoEstado"),
    F.col("Oportunidad").alias("Oportunidad"), # si
    F.col("Oferta").cast(DoubleType()).alias("Oferta"), #si
    F.col("Probabilidad").alias("Probabilidad"), # si
    F.col("Duracion").cast(DoubleType()).alias("Duracion"),# si
    #F.col("ZonaGeografica").cast(DoubleType()).alias("ZonaGeografica"),
    F.col("Coste").cast(DoubleType()).alias("Coste"),# si
    F.col("TCV/Presupuesto").cast(DoubleType()).alias("TCV"),# si
    F.col("ImporteLicitado").cast(DoubleType()).alias("ImporteLicitado"),# si
    F.col("Margen").alias("Margen"),# si
    F.col("FechaPresentacion").alias("FechaPresentacion"),# si
    F.col("Tipologia").alias("Tipologia"), # si
    F.col("Area").alias("Area"),
    F.col("Subarea").alias("Subarea"),
    F.col("Titulo").alias("Titulo"),# si
    F.col("Cliente").alias("Cliente"),# si
    F.col("Sector").alias("Sector"),
    #F.col("Fabricantes").alias("Fabricantes"), --este fabricantes se va a la tabla de dimensiones
    #F.col("FechaCreacion").alias("FechaCreacion"),
    #F.col("FechaArranque").alias("FechaArranque"),
    #F.col("ResponsableMercado").alias("ResponsableMercado"),
    #F.col("ResponsableArea").alias("ResponsableArea"),
    #F.col("Tecnologia").alias("Tecnologia"),    
    #F.col("Renovacion").alias("Renovacion"),
    #F.col("Adjudicatario").alias("Adjudicatario"),
    #F.col("OrigenOportunidad").alias("OrigenOportunidad"),
    F.col("Estado").alias("Estado"), #no pero si, hay que hacer la agrupacion en estado de oportunidades
    F.col("FechaCarga").alias("FechaCarga"),# si
    F.col("FechaArranque").alias("FechaInicio")
)

Fact_CRM_df_oportunidades = Fact_CRM_df_oportunidades.filter(
    (F.col('Oportunidad') == 'Oportunidad Privada') | 
    (F.col('Oportunidad') == 'Oportunidad Pública')
).withColumn(
    'Oportunidad', 
    F.when(F.col('Oportunidad') == 'Oportunidad Privada', 'Privada')
    .when(F.col('Oportunidad') == 'Oportunidad Pública', 'Pública')
    .otherwise(F.col('Oportunidad'))
)
#hacer cambio a que muestre solo pública o privada

Fact_CRM_df_oportunidades = Fact_CRM_df_oportunidades.withColumn("FechaPresentacion", F.date_format(F.col("FechaPresentacion"), "yyyy-MM-dd").cast(DateType()))


# nos vamos a quedar con: IdProy, oportunidad, ofderta, probabilidad, duracion (silver pero no gold), coste (silver pero no gold), presupuesto (TCV), Importe licitado (silver pero no gold), margen (silver pero no gold), fechapresenatcion (es la fecha de filtrar enero 2024), tipologia, título, cliente (podriamos leerlo de unhiberse asi que el que viene nos lo cargariamos y lo cambiariamos por el de unhib) , importe, estado oportunidades (es como estado peero ya agrupado) (muy importante)

# [['IdProyecto','Estado','MotivoEstado','Oportunidad','Area','Subarea','Titulo','Cliente','Sector','Fabricantes','FechaCreacion','Oferta','Probabilidad','Duracion','Coste','TCV/Presupuesto','ImporteLicitado','Margen','FechaArranque','FechaPresentacion','ResponsableMercado','ResponsableArea','Tecnologia','Renovacion','Adjudicatario','Tipologia','EmpresaOfertante','OrigenOportunidad','FechaActualizacion']]

# COMMAND ----------

Fact_CRM_df_oportunidades.distinct().count() # 1083 Oportunidades

# COMMAND ----------

Fact_CRM_df_oportunidades = Fact_CRM_df_oportunidades.withColumn("IdProyecto", F.trim(F.col("IdProyecto")))

# COMMAND ----------

Fact_CRM_df_oportunidades.distinct().count() # 1083 Oportunidades trim

# COMMAND ----------

# DBTITLE 1,Columnas oportunidades
columnas = Fact_CRM_df_oportunidades.columns
columnas

# COMMAND ----------

# DBTITLE 1,gastos con tipo
# MAGIC %sql
# MAGIC create or replace temp view gastos_offering as
# MAGIC select g.* from operaciones.gold_unhiberse.fact_gastos as g
# MAGIC left join operaciones.gold_unhiberse.dim_proyecto as p
# MAGIC on g.IdProyecto = p.IdProyecto
# MAGIC where p.TipoProyecto = 'Proyecto de Inversión - Offering'
# MAGIC

# COMMAND ----------

# DBTITLE 1,count gastos offering
# MAGIC %sql
# MAGIC select count(*) from gastos_offering

# COMMAND ----------

# DBTITLE 1,Agrupar offering gastos
# MAGIC %sql
# MAGIC create or replace temp view auxiliar as
# MAGIC select IdProyecto, sum(ImpGastado) as ImpGastado from gastos_offering group by IdProyecto
# MAGIC

# COMMAND ----------

# DBTITLE 1,Auxiliar agrupado
# MAGIC %sql
# MAGIC select * from auxiliar
# MAGIC group by all
# MAGIC -- 443 rows, idem que Alejandro

# COMMAND ----------

# DBTITLE 1,silver oportunidades
Fact_CRM_df_oportunidades.createOrReplaceTempView("oportunidades")

spark.sql("""
create or replace temp view oportunidades_2 AS
SELECT 
    op.IdProyecto, 
    op.Oportunidad,
    op.Oferta, 
    op.Probabilidad, 
    op.Duracion,
    aux.ImpGastado as `Inversion`, -- Si no por cada impGastado del proyecto recibiriamos una linea duplicada del proyecto. 
    op.TCV as `TCV/Presupuesto`,
    op.ImporteLicitado, -- Es valor propio de oportunidades 
    op.Margen,
    op.FechaPresentacion AS Fecha,
    op.Tipologia,
    op.Titulo,
    CASE 
        WHEN op.Estado = 'GANADA' THEN "Ganadas" 
        WHEN op.Estado = 'NO GANADA' THEN "No ganadas" 
        WHEN op.Estado = 'DESESTIMADA' THEN "Desestimadas" 
        ELSE "Pendientes" 
    END AS `Estado`,
    CASE
        WHEN op.Estado = 'GANADA' THEN  '1'
        WHEN op.Estado = 'NO GANADA' THEN '3'
        WHEN op.Estado = 'DESESTIMADA' THEN '4'
        ELSE '2'
    END AS `OrdenEstado`,
    op.Cliente, 
    op.Sector, 
    op.Area, 
    op.Subarea,
    op.FechaCarga,
    op.FechaInicio
from oportunidades as op
left join auxiliar as aux
    on aux.IdProyecto = op.IdProyecto

""") #gastos_offering

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct IdProyecto, Oferta, Inversion, count(IdProyecto) from oportunidades_2
# MAGIC where Fecha >= '2024-01-01' 
# MAGIC group by IdProyecto, Oferta, Inversion

# COMMAND ----------

# DBTITLE 1,save oportunidades
# Guardar como Delta table
spark.sql("""
    CREATE OR REPLACE TABLE plataformas.silver_crm.oportunidades 
    AS SELECT DISTINCT * FROM oportunidades_2
""")

# Verificar tabla contiene datos
spark.sql("SELECT COUNT(*) FROM plataformas.silver_crm.oportunidades").show()

# COMMAND ----------

# DBTITLE 1,dim fabricantes oportundiades
dim_fabricantes = df_converted_2024.select(
    F.col("IdProyecto").alias("IdProyecto"),
    F.col("Fabricantes").alias("Fabricantes")
)

pattern = r'(\d{4}-\d{4,5}|\d{2}/\d{3})'

# Filter the DataFrame based on the regular expression pattern
dim_fabricantes = dim_fabricantes.filter(F.col('IdProyecto').rlike(pattern))

# List of unwanted values to exclude
exclude_values = ['N/A', 'N/a', 'PDTE', 'Pdte', 'pdte', 'PENDIENTE', 'Pendiente', 'PTE', 'xx', 'XXX', 'XXXX', 'xxxxx', '_temp', '-', '.', '0', '0000', '00000', '1', 'NA', 'na', 'pendiente', '2024', '3000', '10000', '15525', '22500', '29000', '54000', '60000', '100000', '3000000', 'FALTA', 'GIA', 'GLC', 'GLOWCODE', 'GMW', 'GNAVISION', 'GSK', 'GNW', 'GLOC', 'GRPA', 'GPOWER', 'GSHAREPOINT']

# Apply exclusion filter as well
dim_fabricantes = dim_fabricantes.filter(~F.col('IdProyecto').isin(*exclude_values))


df_exploded = dim_fabricantes.withColumn("Fabricantes", F.split(F.col("Fabricantes"), ',')).withColumn("Fabricante", F.explode(F.col("Fabricantes")))
df_exploded_2 = df_exploded.select(
    F.col("IdProyecto"),
    F.trim(F.col("Fabricante")).alias("Fabricante")
) 
# Show the resulting DataFrame
df_exploded_2.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.silver_crm.fabricantes")

# COMMAND ----------

def convert_zona_geografica(zona_geografica):
    if zona_geografica == 14:
        return 'España'
    elif zona_geografica == 30:
        return 'México'
    elif zona_geografica == 18:
        return 'Argentina'
    else:
        return 'Otros'
    
convert_zona_geografica_udf = udf(convert_zona_geografica, StringType())

# COMMAND ----------

# DBTITLE 1,Columnas fact capacidad

Fact_CRM_df_presentacion_capacidades = df_converted_2024.select(
    F.col("Id").alias("Id"),
    F.col("Oportunidad").alias("Oportunidad"), 
    F.col("Area").alias("Area"),
    F.col("Subarea").alias("Subarea"),
    F.col("Titulo").alias("Titulo"),
    F.initcap(F.col("Cliente")).alias("Cliente"),
    F.col("Sector").alias("Sector"),
    F.col("FechaPresentacion").alias("FechaPresentacion"),
    F.col("ZonaGeografica").alias("Zona"),
    F.col("Tecnologia").alias("Tecnologia"), #nos l acargamos, lo traeremos de fabricantespc
    #F.col("Fabricantes").alias("Fabricantes"), #fabricantes no, lo tenedremos en la dim correspondiente
    F.col("FechaCarga").alias("FechaCarga")
)

Fact_CRM_df_presentacion_capacidades = Fact_CRM_df_presentacion_capacidades.filter(
    (Fact_CRM_df_presentacion_capacidades['Oportunidad'] == 'Presentación de Capacidad')
)

Fact_CRM_df_presentacion_capacidades = Fact_CRM_df_presentacion_capacidades.withColumn("FechaPresentacion", F.date_format(F.col("FechaPresentacion"), "yyyy-MM-dd").cast(DateType()))

Fact_CRM_df_presentacion_capacidades = Fact_CRM_df_presentacion_capacidades.withColumn(
    "Zona", 
    convert_zona_geografica_udf(F.col("Zona"))
)

Fact_CRM_df_presentacion_capacidades.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.silver_crm.capacidad") 
#no tenemos los datos de IdProyecto asi que no podemso realizar ningun join

#[['Id','Estado','Oportunidad','Area','Subarea','Titulo','Cliente','Sector','FechaPresentacion','ZonaGeografica','Tecnologia','Fabricantes']]

# COMMAND ----------

# DBTITLE 1,dim fabricantes_pc
dim_fabricantes_pc = df_converted_2024.select(
    F.col("Id").alias("Id"),
    F.col("Fabricantes").alias("Fabricantes")
)

exclude_values = ['N/A', 'N/a', 'PDTE', 'Pdte', 'pdte', 'PENDIENTE', 'Pendiente', 'PTE', 'xx', 'XXX', 'XXXX', 'xxxxx', '_temp', '-', '.', '0', '0000', '00000', '1', 'NA', 'na', 'pendiente' ]

# Filter the DataFrame to exclude rows with unwanted values
dim_fabricantes_pc = dim_fabricantes_pc.filter(~F.col('Id').isin(*exclude_values))

df_exploded = dim_fabricantes_pc.withColumn("Fabricantes", F.split(F.col("Fabricantes"), ',')).withColumn("Fabricante", F.explode(F.col("Fabricantes")))
df_exploded_3 = df_exploded.select(
    F.col("Id").alias("IdSharepoint"),
    F.trim(F.col("Fabricante")).alias("Fabricante")
) 
# Show the resulting DataFrame
df_exploded_3.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.silver_crm.fabricantes_pc")