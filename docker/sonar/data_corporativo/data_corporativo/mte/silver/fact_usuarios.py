# Databricks notebook source
from pyspark.sql.functions import col, when, date_format, to_date, regexp_replace

# COMMAND ----------

# Read table from catalog
df = spark.read.table("operaciones.bronze_mte.fact_usuarios")

# Show the first few rows of the dataframe
#df.display()

# COMMAND ----------

# Transformación de columnas booleanas en enteros 1/0
df = df.withColumn("Imputa", when(col("Imputa") == True, 1).otherwise(0))
df = df.withColumn("Reasignable", when(col("Reasignable") == True, 1).otherwise(0))
df = df.withColumn("Bonificable", when(col("Bonificable") == True, 1).otherwise(0))
# Remove percentage symbol and convert to decimal
df = df.withColumn("Productividad", regexp_replace(col("Productividad"), "%", ""))
df = df.withColumn("Productividad", (col("Productividad").cast("float") / 100))

# Se decide mantener las celdas vacias como NULL
df = df.withColumn('IdResponsablehBU', when(col('IdResponsablehBU') == "", None).otherwise(col('IdResponsablehBU')))
df = df.withColumn('IdResponsablehMA', when(col('IdResponsablehMA') == "", None).otherwise(col('IdResponsablehMA')))
df = df.withColumn('IdResponsableEquipoTrabajoMultiarea', when(col('IdResponsableEquipoTrabajoMultiarea') == "", None).otherwise(col('IdResponsableEquipoTrabajoMultiarea')))
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

#df.display()

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

#df.display()

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# Columna bool EsBaja si FechaBaja no es null
df = df.withColumn("EsBaja", when(col("FechaBaja").isNotNull(), 1).otherwise(lit(0)))

df = df.drop("ComunidadesTecnologicas")

#df.display()

# COMMAND ----------

# Save to SILVER catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.fact_usuarios')

df_carreraprofesional = df.select("IdCarreraProfesional", "CarreraProfesional").distinct().dropna()
df_gruponivel = df.select("IdGrupoNivel", "GrupoNivel").distinct().dropna()
df_nivelprofesional = df.select("IdNivelProfesional", "NivelProfesional").distinct().dropna()

df_carreraprofesional.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.CarreraProfesional')
df_gruponivel.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.GrupoNivel')
df_nivelprofesional.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('operaciones.silver_mte.NivelProfesional')

# Save to PEOPLE catalog
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.fact_usuarios')

df_carreraprofesional.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.CarreraProfesional')
df_gruponivel.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.GrupoNivel')
df_nivelprofesional.write.mode("overwrite").format("delta").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('people.silver_mte.NivelProfesional')