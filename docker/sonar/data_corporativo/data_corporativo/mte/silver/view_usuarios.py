# Databricks notebook source
from pyspark.sql.functions import col, when, date_format, to_date, regexp_replace

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.bronze_mte.view_usuarios")

# Show the first few rows of the dataframe
# df.display()

# COMMAND ----------

# Transformación de columnas booleanas en enteros 1/0
df = df.withColumn("Imputa", when(col("Imputa") == True, 1).otherwise(0))
df = df.withColumn("Reasignable", when(col("Reasignable") == True, 1).otherwise(0))
df = df.withColumn("Bonificable", when(col("Bonificable") == True, 1).otherwise(0))
# Remove percentage symbol and convert to decimal
df = df.withColumn("Productividad", regexp_replace(col("Productividad"), "%", ""))
df = df.withColumn("Productividad", (col("Productividad").cast("float") / 100))

# Se decide mantener las celdas vacias como NULL
df = df.withColumn('ResponsablehBU', when(col('ResponsablehBU') == "", None).otherwise(col('ResponsablehBU')))
df = df.withColumn('ResponsablehMA', when(col('ResponsablehMA') == "", None).otherwise(col('ResponsablehMA')))
df = df.withColumn('ResponsableEquipoTrabajoMultiarea', when(col('ResponsableEquipoTrabajoMultiarea') == "", None).otherwise(col('ResponsableEquipoTrabajoMultiarea')))

# Columnas fecha a formato dd-mm-yyyy
df = df.withColumn("FechaAlta", date_format(col("FechaAlta"),"dd-MM-yyyy"))
df = df.withColumn("FechaAntiguedad", date_format(col("FechaAntiguedad"),"dd-MM-yyyy"))
df = df.withColumn("FechaBaja", date_format(col("FechaBaja"),"dd-MM-yyyy"))
df = df.withColumn("FechaNacimiento", date_format(col("FechaNacimiento"),"dd-MM-yyyy"))
df = df.withColumn("FechaUltimoCambioSalarial", date_format(col("FechaUltimoCambioSalarial"),"dd-MM-yyyy"))
df = df.withColumn("FechaUltimaRevisionSalarial", date_format(col("FechaUltimaRevisionSalarial"),"dd-MM-yyyy"))

df = df.withColumn("FechaAlta", to_date(df["FechaAlta"], "dd-MM-yyyy"))
df = df.withColumn("FechaAntiguedad", to_date(df["FechaAntiguedad"], "dd-MM-yyyy"))
df = df.withColumn("FechaBaja", to_date(df["FechaBaja"], "dd-MM-yyyy"))
df = df.withColumn("FechaNacimiento", to_date(df["FechaNacimiento"], "dd-MM-yyyy"))
df = df.withColumn("FechaUltimoCambioSalarial", to_date(df["FechaUltimoCambioSalarial"], "dd-MM-yyyy"))
df = df.withColumn("FechaUltimaRevisionSalarial", to_date(df["FechaUltimaRevisionSalarial"], "dd-MM-yyyy"))

# COMMAND ----------

# df.display()

# COMMAND ----------

# from pyspark.sql.functions import regexp_extract
# from pyspark.sql.types import IntegerType
# # Funcion para extraer el id Grupo/Nivel/Categoria
# def extract_number(df, column_name):
#     new_column_name = f"ID_{column_name}"
#     # Use regular expression to extract number
#     df = df.withColumn(new_column_name, regexp_extract(col(column_name), r"(\d+\.?\d*)", 1))
#     # Cast to integer
#     df = df.withColumn(new_column_name, df[new_column_name].cast(IntegerType()))
#     return df

# df = extract_number(df, "GrupoNivel")
# df = extract_number(df, "NivelProfesional")

# df.display()

# COMMAND ----------

from pyspark.sql.functions import current_date, datediff, floor , to_date

# Columna calculada edad
df = df.withColumn("FechaNacimiento", to_date(col("FechaNacimiento"), "dd-MM-yyyy"))
df = df.withColumn("Edad", floor(datediff(current_date(), col("FechaNacimiento")) / 365.25))

# Agregar RangoEdad
df = df.withColumn(
    "RangoEdad",
    when(col("Edad") >= 61, "61 o más")
    .when((col("Edad") >= 56) & (col("Edad") <= 60), "56-60")
    .when((col("Edad") >= 51) & (col("Edad") <= 55), "51-55")
    .when((col("Edad") >= 46) & (col("Edad") <= 50), "46-50")
    .when((col("Edad") >= 41) & (col("Edad") <= 45), "41-45")
    .when((col("Edad") >= 36) & (col("Edad") <= 40), "36-40")
    .when((col("Edad") >= 31) & (col("Edad") <= 35), "31-35")
    .when((col("Edad") >= 26) & (col("Edad") <= 30), "26-30")
    .when((col("Edad") >= 21) & (col("Edad") <= 25), "21-25")
    .when((col("Edad") >= 18) & (col("Edad") <= 20), "18-20")
    .otherwise("Menor de 18"))

# Columna Zona
df = df.withColumn("Zona", when(col("hGZ") == "España", "España").otherwise("Internacional"))

# Columna calculada años antigüedad
df = df.withColumn("FechaAntiguedad", to_date(col("FechaAntiguedad"), "dd-MM-yyyy"))
df = df.withColumn("AñosAntiguedad", floor(datediff(current_date(), col("FechaAntiguedad")) / 365.25))

# Rango antigüedad
df = df.withColumn(
    "RangoAntiguedad",
    when(col("AñosAntiguedad") >= 10, "+10 años")
    .when((col("AñosAntiguedad") >= 5) & (col("AñosAntiguedad") < 10), "Menos de 10 años")
    .when((col("AñosAntiguedad") >= 3) & (col("AñosAntiguedad") < 5), "Menos de 5 años")
    .when((col("AñosAntiguedad") >= 1) & (col("AñosAntiguedad") < 3), "Menos de 3 años")
    .otherwise("Menos de 1 año"))

# df.display()

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# Columna bool EsBaja si FechaBaja no es null
df = df.withColumn("EsBaja", when(col("FechaBaja").isNotNull(), 1).otherwise(lit(0)))

display(df)

# COMMAND ----------

# DBTITLE 1,explode bajas
# from pyspark.sql import functions as F
# from pyspark.sql.types import *
# import re

# # Register UDFs to extract values using regex patterns
# @F.udf(returnType=StringType())
# def extract_origen_baja_value(bajas_array):
#     if bajas_array is None:
#         return None
    
#     # Convert array to string for regex processing
#     bajas_str = str(bajas_array)
    
#     # Pattern to extract 'Value' from OrigenBaja
#     pattern = r"'OrigenBaja':\s*\{[^}]*'Value':\s*'([^']*)'[^}]*\}"
#     matches = re.findall(pattern, bajas_str)
    
#     if not matches:
#         return None
    
#     return ", ".join(matches)

# @F.udf(returnType=StringType())
# def extract_tipo_baja_value(bajas_array):
#     if bajas_array is None:
#         return None
    
#     bajas_str = str(bajas_array)
#     pattern = r"'TipoBaja':\s*\{[^}]*'Value':\s*'([^']*)'[^}]*\}"
#     matches = re.findall(pattern, bajas_str)
    
#     if not matches:
#         return None
    
#     return ", ".join(matches)

# @F.udf(returnType=StringType())
# def extract_observaciones(bajas_array):
#     if bajas_array is None:
#         return None
    
#     bajas_str = str(bajas_array)
#     pattern = r"'Observaciones':\s*'([^']*)'"
#     matches = re.findall(pattern, bajas_str)
    
#     if not matches:
#         return None
    
#     return ", ".join(matches)

# @F.udf(returnType=BooleanType())
# def extract_mantener_contacto(bajas_array):
#     if bajas_array is None:
#         return None
    
#     bajas_str = str(bajas_array)
#     pattern = r"'MantenerContacto':\s*(True|False)"
#     matches = re.findall(pattern, bajas_str)
    
#     if not matches:
#         return None
    
#     # Return True if any value is True
#     return "True" in matches

# @F.udf(returnType=StringType())
# def extract_aprobador_rrhh_id(bajas_array):
#     if bajas_array is None:
#         return None
    
#     bajas_str = str(bajas_array)
#     pattern = r"'AprobadorRRHH':\s*\{[^}]*'Id':\s*(\d+)[^}]*\}"
#     matches = re.findall(pattern, bajas_str)
    
#     if not matches:
#         return None
    
#     return ", ".join(matches)

# # Apply the UDFs to extract the values
# result_df = df.withColumn("OrigenBaja_Value", extract_origen_baja_value(F.col("Bajas")))
# result_df = result_df.withColumn("TipoBaja_Value", extract_tipo_baja_value(F.col("Bajas")))
# result_df = result_df.withColumn("Observaciones", extract_observaciones(F.col("Bajas")))
# result_df = result_df.withColumn("MantenerContacto", extract_mantener_contacto(F.col("Bajas")))
# result_df = result_df.withColumn("AprobadorRRHH_Id", extract_aprobador_rrhh_id(F.col("Bajas")))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import ast

# Define all possible fields that we want to extract from the Bajas column
# Each tuple contains (field_name, nested_path, data_type)
bajas_fields = [
    ("Referencia", None, StringType()),
    ("EstadoPeticion", None, StringType()),
    ("FechaBaja", None, StringType()),
    ("FechaDevMat", None, StringType()),
    ("OrigenBaja_Id", ["OrigenBaja", "Id"], IntegerType()),
    ("OrigenBaja_Value", ["OrigenBaja", "Value"], StringType()),
    ("TipoBaja_Id", ["TipoBaja", "Id"], IntegerType()),
    ("TipoBaja_Value", ["TipoBaja", "Value"], StringType()),
    ("MotivoBaja_Id", ["MotivoBaja", "Id"], IntegerType()),
    ("MotivoBaja_Value", ["MotivoBaja", "Value"], StringType()),
    ("Observaciones", None, StringType()),
    ("FechaRespuesta", None, StringType()),
    ("MantenerContacto", None, BooleanType()),
    ("TelefonoContacto", None, StringType()),
    ("EmailContacto", None, StringType()),
    ("Sugerencia", None, StringType()),
    ("AprobadorRRHH_Id", ["AprobadorRRHH", "Id"], IntegerType()),
    ("AprobadorRRHH_Value", ["AprobadorRRHH", "Value"], StringType()),
    ("AprobadorRRHH_Email", ["AprobadorRRHH", "Email"], StringType()),
    ("FechaAprobacionRRHH", None, StringType()),
    ("ComentariosAprobacionRRHH", None, StringType()),
    ("ID", None, IntegerType()),
    ("Usuario_Id", ["Usuario", "Id"], IntegerType()),
    ("Usuario_Value", ["Usuario", "Value"], StringType())
]

def parse_bajas_entry(entry):
    """Parse a single Bajas entry dictionary"""
    result = {}
    
    for field_name, nested_path, _ in bajas_fields:
        if nested_path is None:
            # Direct field access
            result[field_name] = entry.get(field_name)
        else:
            # Nested field access
            try:
                value = entry
                for path_part in nested_path:
                    if value is not None and path_part in value:
                        value = value[path_part]
                    else:
                        value = None
                        break
                result[field_name] = value
            except (KeyError, TypeError, AttributeError):
                result[field_name] = None
    
    return result

@F.udf(returnType=ArrayType(MapType(StringType(), StringType())))
def extract_all_bajas_data(bajas_array):
    """
    Extract all data from the Bajas array column.
    Returns an array of maps, each containing all the extracted fields for one entry.
    """
    if bajas_array is None:
        return None
    
    try:
        # Try to convert the string representation to a Python object
        if isinstance(bajas_array, str):
            # Handle string format cases
            try:
                # First try as a JSON string
                entries = json.loads(bajas_array)
            except json.JSONDecodeError:
                # Then try as a Python literal
                try:
                    entries = ast.literal_eval(bajas_array)
                except (SyntaxError, ValueError):
                    # If it's a single entry as string, wrap it in a list
                    try:
                        single_entry = ast.literal_eval(bajas_array)
                        entries = [single_entry]
                    except:
                        # Last resort: try regex-based extraction (simplified)
                        return None
        else:
            # Already a Python object (list, dict)
            entries = bajas_array
        
        # Ensure entries is a list
        if not isinstance(entries, list):
            entries = [entries]
        
        # Process each entry
        results = []
        for entry in entries:
            if isinstance(entry, str):
                try:
                    # Try to parse string representation of dict
                    entry = ast.literal_eval(entry)
                except:
                    continue
            
            # Parse the entry and convert all values to strings for the result map
            parsed = parse_bajas_entry(entry)
            string_map = {k: str(v) if v is not None else None for k, v in parsed.items()}
            results.append(string_map)
            
        return results
    except Exception as e:
        # Return empty list on any error
        return []

# UDF to extract a specific field from the parsed data
def create_extract_field_udf(field_name, return_type):
    @F.udf(returnType=return_type)
    def extract_field(parsed_data):
        if parsed_data is None or len(parsed_data) == 0:
            return None
        
        # Join multiple values with commas if there are multiple entries
        values = [entry.get(field_name) for entry in parsed_data if entry.get(field_name) is not None]
        
        if not values:
            return None
            
        # Convert boolean strings to actual booleans if needed
        if return_type == BooleanType():
            return any(v.lower() == "true" for v in values if v)
        
        # Convert to integers if the type is IntegerType
        if return_type == IntegerType():
            try:
                # Try to return the first valid integer
                for v in values:
                    if v and v.strip():
                        return int(v)
                return None
            except ValueError:
                return None
        
        # For string type, join all values
        return ", ".join(v for v in values if v)
    
    return extract_field

# Create a new dataframe for bajas without affecting the original dataframe
temp_df = df.withColumn("ParsedBajas", extract_all_bajas_data(F.col("Bajas")))

# Create a builder dataframe that we'll use to construct our final bajas_df
builder_df = temp_df

# Create individual columns for each field
for field_name, _, return_type in bajas_fields:
    extract_field_udf = create_extract_field_udf(field_name, return_type)
    builder_df = builder_df.withColumn(field_name, extract_field_udf(F.col("ParsedBajas")))

# Create the final bajas_df with only the columns we need
bajas_df = builder_df.select(
    F.col("Usuario_Id").alias("IdUsuario"), 
    F.col("DNI"), 
    F.col("FechaAlta"), 
    F.col("EstadoPeticion"), 
    F.col("FechaBaja").cast(DateType()),
    F.col("OrigenBaja_Value").alias("OrigenBaja"), 
    F.col("TipoBaja_Value").alias("TipoBaja"), 
    F.col("MotivoBaja_Value").alias("MotivoBaja"), 
    F.col("AprobadorRRHH_Value").alias("AprobadorRRHH"), 
    F.col("Observaciones"), 
    F.col("ComentariosAprobacionRRHH").alias("ComentariosAprobadorRRHH")
).filter(F.col("EsBaja") == 1)

# Display the new dataframe
bajas_df.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Create a copy of the original dataframe
result_df = df

# Correct the join operation
result_df = result_df.join(
    bajas_df.select(
        F.col("IdUsuario"),  # Include this column for joining
        F.col("OrigenBaja"),
        F.col("TipoBaja"),
        F.col("MotivoBaja"),
        F.col("Observaciones"),
        F.col("ComentariosAprobadorRRHH")
    ),
    result_df.ID == bajas_df.IdUsuario,  # Correct join condition
    how="left"
)
result_df = result_df.drop("IdUsuario")
# Display the result
result_df.display()

# en view dejar las 5 columnas que hay en la tarea de planner "Revisar view_usuarios", actualizar en gold lo otro

# COMMAND ----------

bajas_df = bajas_df.filter(F.col("IdUsuario").isNotNull())
df = result_df

# COMMAND ----------

df_tarifacoste = df.select("ID", "TarifaCoste").distinct()


df_tarifacoste.display()


# COMMAND ----------

# Save to SILVER catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.view_usuarios')

df_tarifacoste.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.TarifaCoste')

bajas_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.bajas')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.view_usuarios')

df_tarifacoste.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.TarifaCoste')

bajas_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.bajas')