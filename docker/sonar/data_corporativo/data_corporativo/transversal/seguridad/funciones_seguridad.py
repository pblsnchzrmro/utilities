# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION transversal.gold_seguridad.user_filter(project STRING)
# MAGIC RETURNS BOOLEAN 
# MAGIC RETURN  (
# MAGIC CASE 
# MAGIC         WHEN CURRENT_USER() = 'jarodriguez@hiberus.com' OR CURRENT_USER = 'dmartinez@hiberus.com'  OR CURRENT_USER = 'jamolina@hiberus.com' or CURRENT_USER = 'psanchezr@hiberus.com' THEN TRUE
# MAGIC         ELSE EXISTS (
# MAGIC             SELECT 1 FROM transversal.gold_seguridad.seguridad
# MAGIC             WHERE Permiso = CURRENT_USER() AND IdProyecto = project
# MAGIC         )
# MAGIC     END
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE operaciones.gold_unhiberse.fact_facturas SET ROW FILTER transversal.gold_seguridad.user_filter ON (IdProyecto);
# MAGIC ALTER TABLE operaciones.gold_unhiberse.fact_incurridos SET ROW FILTER transversal.gold_seguridad.user_filter ON (IdProyecto);
# MAGIC ALTER TABLE operaciones.gold_unhiberse.fact_gastos SET ROW FILTER transversal.gold_seguridad.user_filter ON (IdProyecto);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE operaciones.gold_kvp.fact_asignaciones SET ROW FILTER transversal.gold_seguridad.user_filter ON (IdProyecto);
# MAGIC ALTER TABLE operaciones.gold_kvp.fact_imputaciones SET ROW FILTER transversal.gold_seguridad.user_filter ON (IdProyecto);