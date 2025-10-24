# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS plataformas.gold_crm")

# COMMAND ----------

# query = spark.sql("select distinct * from plataformas.silver_crm.capacidad")
# query.display()

# COMMAND ----------

# DBTITLE 1,fact_capacidades
capacidad_df = spark.sql("SELECT DISTINCT * FROM plataformas.silver_crm.capacidad")

capacidad_df = capacidad_df.select(
    F.col("Id").alias("IdSharepoint"),
    F.col("Area"),
    F.col("Subarea"), 
    F.col("Titulo"), 
    F.col("Cliente"),
    F.col("Sector"),
    F.col("FechaPresentacion").alias("Fecha"), 
    F.col("Zona"), 
    F.col("FechaCarga")

)

capacidad_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.gold_crm.fact_capacidades")

# COMMAND ----------

# query = spark.sql("select distinct * from plataformas.silver_crm.oportunidades")
# query.display()

# COMMAND ----------

# DBTITLE 1,fact_oportunidades
oportunidades_df = spark.sql("SELECT DISTINCT * FROM plataformas.silver_crm.oportunidades")

oportunidades_df = oportunidades_df.select(
    F.col("IdProyecto"),
    F.col("Oportunidad").alias("Tipo"), 
    F.col("Oferta"),
    (F.when(F.col('Probabilidad').rlike(r'^\d+%$'), 
            F.regexp_replace(F.col('Probabilidad'), '%', '').cast('float') / 100)
     .otherwise(F.col('Probabilidad').cast('double'))).alias('Probabilidad'),
    F.col("Inversion"), 
    F.col("TCV/Presupuesto").alias("Presupuesto"),
    F.col("Duracion"),
    F.col("Fecha"),
    F.col("Titulo"),
    F.col("Cliente"),
    F.col("Sector"),
    F.col("Area"),
    F.col("Subarea"),
    F.col("Estado"),
    F.col("OrdenEstado"),
    F.col("FechaCarga"),
    F.col("FechaInicio").cast('date').alias("FechaInicio")
)


oportunidades_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.gold_crm.fact_oportunidades")

# COMMAND ----------

# DBTITLE 1,fact_previones
previsiones_df = spark.sql(
    """SELECT
            CAST(date_trunc('month', add_months(FechaInicio, MesProyecto - 1)) AS DATE) as Fecha,
            IdProyecto,
            Importe
            FROM (
                SELECT 
                FechaInicio,
                IdProyecto,
                CASE WHEN Estado = 'Ganadas' THEN ROUND(Oferta/Duracion, 2) ELSE ROUND((Oferta * (CAST(REPLACE(Probabilidad, '%', '') AS INT) / 100))/Duracion, 2) END AS Importe,
                explode(sequence(1, CAST(Duracion AS INTEGER))) AS MesProyecto
                FROM plataformas.silver_crm.oportunidades 
                WHERE (Estado = 'Ganadas' OR (Estado = 'Pendientes' AND CAST(REPLACE(Probabilidad, '%', '') AS INT) >= 75)) 
                AND IdProyecto LIKE '%-%' AND IdProyecto != '-'
                AND Duracion > 0 and Duracion < 200
                AND Oferta != 0
            )"""
)
previsiones_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.gold_crm.fact_previsiones")

# COMMAND ----------

# DBTITLE 1,dim_fabricante
fabricantes_df = spark.sql("SELECT DISTINCT * FROM plataformas.silver_crm.fabricantes")
fabricantes_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.gold_crm.dim_fabricante")

# COMMAND ----------

# DBTITLE 1,dim_fabricantepc
fabricantes_pc_df = spark.sql("SELECT DISTINCT * FROM plataformas.silver_crm.fabricantes_pc")
fabricantes_pc_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("plataformas.gold_crm.dim_fabricantepc")