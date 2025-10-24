# Databricks notebook source
# MAGIC %md
# MAGIC Trabajo de las tablas,  no hay que definir entorno ni llamar Utils/UDFs porque las tablas se encuentran cargadas en operaciones.silver_unhiberse

# COMMAND ----------

from datetime import datetime, timedelta
import calendar
import time
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS operaciones.gold_unhiberse")

# COMMAND ----------

# DBTITLE 1,Drop schema
#spark.sql("DROP SCHEMA IF EXISTS operaciones.gold_unhiberse CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC Tablas de Gold a partir de las silver renombradas y con nombres de columnas ajustados para la visualizacion final

# COMMAND ----------

# DBTITLE 1,Clientes (Dim)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dim_clientes AS
# MAGIC SELECT DISTINCT
# MAGIC     client.id AS `IdCliente`, --correcto, pasar de alfanumerico a numerico
# MAGIC     INITCAP(TRIM(client.name)) AS `Cliente`, --correcto
# MAGIC     --INITCAP(TRIM(group.name)) AS `GrupoCliente`,
# MAGIC     INITCAP(TRIM(client.company_name)) AS `Empresa`, --correcto
# MAGIC     --arabia_client_type.name AS `Tipo_Cliente_Arabia`, --join con arabia_client_type
# MAGIC     ue_group.name AS `GrupoUE`, -- join con ue_group
# MAGIC     hgz.Nombre AS `Pais`, -- join con country
# MAGIC     client.business_address AS `Direccion`, --correcto
# MAGIC     client.postal_code AS `CP`, --correcto
# MAGIC     client.cif AS `CIF`, -- correcto, no tiene join
# MAGIC     --client.original_id AS `OriginalID`, --join con ????
# MAGIC     --DATE_FORMAT(client.created, 'yyyy-MM-dd HH:mm:ss') AS `Cliente_Creado`, --best practices podriamos elimianr HH:mm:ss y consumiriamos menos a la hora de cargar los datos, preguntar alejandro que formato de fecha queremos
# MAGIC     --DATE_FORMAT(client.updated, 'yyyy-MM-dd HH:mm:ss') AS `Cliente_Actualizado`,
# MAGIC     --CASE WHEN client.inactive THEN 0 ELSE 1 END AS `ClienteActivo`, --salida en vez de true or false (podria ponerse 'Activo' o 'Inactivo' en vez de 1 o 0 si se quisiera más user-friendly)
# MAGIC     client_type.name AS `TipoCliente` --join con client_type 
# MAGIC     --mte_company.Nombre AS `Codigo_Empresa_Representante`, --join con bronze_mte.empresas
# MAGIC     --client.shipping_address AS `Direccion_Envio` --correcto
# MAGIC FROM operaciones.silver_unhiberse.client AS client
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project as project
# MAGIC   ON client.id = project.client_id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.client_group as group 
# MAGIC   ON project.client_group_id = group.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.arabia_client_type AS arabia_client_type
# MAGIC   ON  arabia_client_type.code = client.arabia_client_type_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.ue_group AS ue_group
# MAGIC   ON  ue_group.code = client.ue_group_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hgz AS hgz
# MAGIC     ON hgz.ID = client.country_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.client_type AS client_type
# MAGIC   ON client_type.code = client.type_code
# MAGIC LEFT JOIN operaciones.bronze_mte.empresas AS mte_company
# MAGIC   ON mte_company.ID = client.behalf_company_code
# MAGIC
# MAGIC WHERE client.inactive = false;
# MAGIC -- para comprobar podriamos sacar los clientes de unhiberse
# MAGIC
# MAGIC --codigo_empresa_representante (behalf company code) no es de client related company (solo hay id ahi)
# MAGIC --posiblemnete behalf_company_code sea join con mte_company (mte_company.code = client.behalf_company_code y llamar a mte_comapny.name)
# MAGIC
# MAGIC --ue_group, country, arabia_client_type y client_type cargadas como tablas auxiliares 
# MAGIC
# MAGIC --Revisar espacios que no entren en los nombres, y mayus minus

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentarios clientes: 
# MAGIC -- cliente id lo quitamos, pasamos el alfanumérico a un id numérico
# MAGIC -- cliente nombre --> Nombre Cliente, y en el de company poner Nombre Empresa
# MAGIC -- tipo cliente arabia no sirve finalmente
# MAGIC -- Direccion Negocio -> Direccion
# MAGIC -- codigo postal -> CP
# MAGIC -- client original id, cliente creado, actualziado no sirve finalmente
# MAGIC -- girar cliente inactivo a activo 
# MAGIC -- codigo empresa representando y direccion envio no sirve finalmente

# COMMAND ----------

# DBTITLE 1,Proyectos (Dim) v1
# %sql
# CREATE OR REPLACE TABLE operaciones.gold_unhiberse.dim_proyectos AS
# SELECT 
#     --project.id AS `IdProyecto`,

#     project.code AS `IdProyecto`,
#     --project.name AS `Nombre`,
#     INITCAP(TRIM(CONCAT(project.code, ': ', project.name))) AS `Proyecto`,
#     INITCAP(TRIM(CONCAT(parent_project.code, ': ', parent_project.name))) AS `ProyectoPadre`,

#     --project.description AS `Descripcion`,

#     project_activity_type.name AS `TipoActividad`,
#     project_type.name AS `TipoProyecto`,
#     project_state.name AS `EstadoProyecto`,

#     --project.original_id, --el original id no sirve, como en cliente 

#     CAST(DATE_FORMAT(project.created, 'yyyy-MM-dd') AS DATE) AS `FechaCreacion`, --si ponemos yyyy-MM-dd mantiene formato date para que lo lea mas tarde, si obligamos a que el formato sea dd-MM-yyyy perdemos date y pasamos a String 
#     --CAST(DATE_FORMAT(project.updated, 'yyyy-MM-dd') AS DATE) AS `ProyectoActualizado`,

#     CASE WHEN project.inactive THEN 0 ELSE 1 END AS `ProyectoActivo`, --flag de actividad, cambiado a positivo
#     project_billing_type.name AS `TipoFacturacion`, --join project_billing_type.name
    
#     -- Joins con tablas de mte:
#     empresas.Nombre AS `NombreEmpresa`, --unhiberse.company_code = mte_comp.code
#     hgz.Nombre AS `Zona`, --unhiberse.zone_code = mte_zone.code
#     --hgr.Nombre AS `Region`, --unhiberse.region_code = mte_reg.code
#     --subhgr.Nombre AS `SubRegion`, --unhiberse.sub_region_code = mte_sub_reg.code
#     CASE 
#         WHEN hgr.Nombre IS NULL OR TRIM(hgr.Nombre) = '' THEN hgz.Nombre
#         ELSE hgr.Nombre
#     END AS `Region`,
#     CASE 
#         WHEN subhgr.Nombre IS NULL OR TRIM(subhgr.Nombre) = '' THEN 
#             CASE 
#                 WHEN hgr.Nombre IS NULL OR TRIM(hgr.Nombre) = '' THEN 
#                     hgz.Nombre
#                 ELSE 
#                     hgr.Nombre
#             END
#         ELSE 
#             subhgr.Nombre
#     END AS `SubRegion`,

#     hma.Nombre AS `HMA`, --unhiberse.hma_code = mte_hma.code
#     hbu.Nombre AS `HBU`, --unhiberse.hbu_code = mte_hbu.code
#     subhbu.Nombre as `SubHBU`, --unhiberse.sub_hbu_code = mte_subhbu.code
#     sector.Nombre AS `Sector` , --unhiberse.sector_code = mte_sector.code

#     --el codigo sector se unirá de la tabla sector en gold, si no se puede coger de hbu, ya que comparten codigo para hBU y Sector. 
#     /*
#     CASE
#         WHEN Sector IN ('AAPP', 'SANIDAD', 'DEFENSA', 'Educación') THEN 'Organismos Públicos'
#         WHEN Sector IN ('TELCO', 'MEDIA') THEN 'Media'
#         WHEN Sector IN ('RETAIL', 'SERVICIOS', 'INDUSTRIA', 'UTILITIES', 'Servicios y AECO') THEN 'Retail'
#         WHEN Sector IN ('SEGUROS', 'BANCA') THEN 'Servicios Financieros'
#         WHEN Sector IN ('AEROLINEAS', 'TRAVEL', 'TRANSPORTE', 'OCIO') THEN 'Travel'
#         WHEN Sector = 'INTERNO' THEN 'SMB'
#         ELSE 'Desconocido'  -- Si alguna categoría no entra en los casos definidos
#     END AS `Familia/Sector`,
#     */
#     -- Market se cogera de la SILVER market de MTE

#     -- Cambiar Familia/Sector por una tabla auxiliar que coja el id del sector/market, y coja el nombre de las 6 opciones de hMA y no esta chapuza manual.

#     CASE 
#         WHEN Sector = 'INTERNO' and market.Nombre IS NULL THEN 'SMB'
#         ELSE market.Nombre 
#     END AS `Mercado`,
#     --category_market.name AS `Mercado`, --unhiberse.market_code = mte_market.code
    
#     --Otros joins: 

#     --payment_condition.name AS `Condicion_Pago`,
#     --project.total_real_amount AS `Monto_Total_Real`,
#     --project.total_estimated_amount AS `Monto_Total_Estimado`,
    
#     client.id AS `IdCliente`,
#     --client.name AS `Cliente`,
#     client_group.id AS `IdGrupoCliente`,
#     --client_group.name AS `GrupoCliente`, --client_group no hace falta, pueden unir proyecto con cliente y ya obtienen de ahí el grupo cliente
#     /* Esto de Grupo Sin Nulls al final no lo usamos
#     CASE 
#         WHEN client_group.name = '' OR client_group.name IS NULL THEN 'Desconocido/Sin Cliente_Proyecto'
#         ELSE client_group.name
#     END AS `GrupoS/Nulls`,
#     */


#     /*
#     sales_person.name AS `Comercial`, --cuando esté lista la tabla sales_person
#     */

#     --project.limit_amount AS `Monto_Limite`,
#     equipostrabajo.Nombre AS `EquipoTrabajo`,
    
#     CAST(DATE_FORMAT(project.start_date, 'yyyy-MM-dd') AS DATE) AS `FechaInicio`, 
#     CAST(DATE_FORMAT(project.end_date, 'yyyy-MM-dd') AS DATE) AS `FechaFin`,
    
#     --parent_project.name AS `Proyecto_Padre`, --principal_parent_id, puede ser usefull si existe.
    
#     --project.comments AS `Comentarios_Proyecto`, --podria omitirse

#     CASE WHEN project.atenea_project THEN 1 ELSE 0 END AS `Atenea`, --flag si es atenea, girado a positivo
#     --project.user_created, --si es atenea tiene sus fechas
#     --project.user_updated, 
#     --project.atenea_budget_code, --si es atenea tiene un budget code de alguna manera (buscar l atabla atenea correspondiente para join y ver el monto del budget, no el codigo)

#     CASE WHEN project.is_billable THEN 1 ELSE 0 END AS `EsFacturable` --girado a positivo
    
#     --project.bdm_user_code AS `Codigo_BDM` --no nos sivre

# FROM operaciones.silver_unhiberse.project AS project

# --Joins de projects
# LEFT JOIN operaciones.silver_unhiberse.project_activity_type AS project_activity_type 
#     ON project.project_activity_type_code = project_activity_type.code
# LEFT JOIN operaciones.silver_unhiberse.project_type AS project_type 
#     ON project.project_type_code = project_type.code
# LEFT JOIN operaciones.silver_unhiberse.project_state AS project_state 
#     ON project.project_state_code = project_state.code

# -- Join the client table to get Client Name
# LEFT JOIN operaciones.silver_unhiberse.client AS client 
#     ON project.client_id = client.id

# -- Join the client_group table to get Client Group Name
# LEFT JOIN operaciones.silver_unhiberse.client_group AS client_group 
#     ON project.client_group_id = client_group.id

# -- Join the empresas table to get Company Name
# LEFT JOIN operaciones.bronze_mte.empresas AS empresas
#   ON empresas.ID = project.company_code

# --Join the hma table to get the HMA name
# LEFT JOIN operaciones.bronze_mte.hma AS hma
#     ON hma.ID = project.hma_code

# --Join the hbu table to get the HBU name, and sector
# LEFT JOIN operaciones.bronze_mte.hbu AS hbu
#  ON hbu.ID = project.hbu_code
# LEFT JOIN operaciones.bronze_mte.hbu AS sector
#     ON sector.ID = project.sector_code
# --el id de hbu/sector en hbu y quiero unir eso para que me ponga las columnas de hma con el flag isMarket = true
# LEFT JOIN operaciones.silver_unhiberse.mercado_auxiliar as market
#     on market.id_hbu = project.sector_code

# -- Join the subhbu table to get the SubHBU name
# LEFT JOIN operaciones.bronze_mte.subhbu AS subhbu
#   ON project.sub_hbu_code = subhbu.ID

# -- Join project_billing_type
# LEFT JOIN operaciones.silver_unhiberse.project_billing_type AS project_billing_type
#     ON project.project_billing_type_code = project_billing_type.code

# LEFT JOIN operaciones.bronze_mte.equipostrabajohbu AS equipostrabajo
#     ON equipostrabajo.ID = project.work_team_code

# LEFT JOIN operaciones.bronze_mte.hgr AS hgr
#     ON hgr.ID = project.region_code
# LEFT JOIN operaciones.bronze_mte.subhgr AS subhgr
#     ON subhgr.ID = project.sub_region_code
# LEFT JOIN operaciones.bronze_mte.hgz AS hgz
#     ON hgz.ID = project.zone_code

# LEFT JOIN operaciones.silver_unhiberse.payment_condition AS payment_condition
#     ON project.payment_condition_id = payment_condition.id

# LEFT JOIN operaciones.silver_unhiberse.category_market AS category_market
#     ON category_market.code = project.market_code

# /* unhiberse.sales_person_code = sales_person.code
# -- Join the sales_person table to get the Sales Person's Name (cuando obtengamos la tabla sales_person) 
# LEFT JOIN operaciones.silver_unhiberse.sales_person AS sales_person 
#     ON project.sales_person_code = sales_person.code
# */
# -- Join the parent project table if `principal_parent_id` exists and you need the Parent Project's Name
# LEFT JOIN operaciones.silver_unhiberse.project AS parent_project 
#     ON project.principal_parent_id = parent_project.id;
#     --hay una tabla que es project_secondary_parent, podemos 




# COMMAND ----------

# DBTITLE 1,Proyectos (Dim) v2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dim_proyectos AS
# MAGIC WITH ParentProjects AS (
# MAGIC     SELECT 
# MAGIC         p1.id AS ProjectID,
# MAGIC         p1.code AS ProjectCode,
# MAGIC         p1.principal_parent_id AS ParentID1,
# MAGIC         p2.code AS ParentProject1,
# MAGIC         p2.principal_parent_id AS ParentID2,
# MAGIC         p3.code AS ParentProject2,
# MAGIC         p3.principal_parent_id AS ParentID3,
# MAGIC         p4.code AS ParentProject3,
# MAGIC         p4.principal_parent_id AS ParentID4,
# MAGIC         p5.code AS ParentProject4,
# MAGIC         p5.principal_parent_id AS ParentID5,
# MAGIC         p6.code AS ParentProject5,
# MAGIC         p6.principal_parent_id AS ParentID6,
# MAGIC         p7.code AS ParentProject6,
# MAGIC         p7.principal_parent_id AS ParentID7,
# MAGIC         
# MAGIC         p2.name AS Parent1Name,
# MAGIC         p3.name AS Parent2Name,
# MAGIC         p4.name AS Parent3Name,
# MAGIC         p5.name AS Parent4Name,
# MAGIC         p6.name AS Parent5Name,
# MAGIC         p7.name AS Parent6Name,
# MAGIC         
# MAGIC         COALESCE(
# MAGIC             CASE WHEN p2.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p3.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p4.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p5.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p6.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p7.id IS NOT NULL THEN 1 ELSE 0 END,
# MAGIC             0
# MAGIC         ) AS NumeroParentProjects
# MAGIC     FROM operaciones.silver_unhiberse.project AS p1
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p2 ON p1.principal_parent_id = p2.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p3 ON p2.principal_parent_id = p3.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p4 ON p3.principal_parent_id = p4.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p5 ON p4.principal_parent_id = p5.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p6 ON p5.principal_parent_id = p6.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p7 ON p6.principal_parent_id = p7.id
# MAGIC )
# MAGIC SELECT 
# MAGIC     -- Columnas originales de dim_proyectos v1
# MAGIC     project.code AS `IdProyecto`,
# MAGIC     INITCAP(TRIM(CONCAT(project.code, ': ', project.name))) AS `Proyecto`,
# MAGIC     --INITCAP(TRIM(CONCAT(parent_project.code, ': ', parent_project.name))) AS `ProyectoPadre`,
# MAGIC     
# MAGIC     project_activity_type.name AS `TipoActividad`,
# MAGIC     project_type.name AS `TipoProyecto`,
# MAGIC     project_state.name AS `EstadoProyecto`,
# MAGIC     
# MAGIC     CAST(DATE_FORMAT(project.created, 'yyyy-MM-dd') AS DATE) AS `FechaCreacion`,
# MAGIC     
# MAGIC     --CASE WHEN project.inactive THEN 0 ELSE 1 END AS `ProyectoActivo`,
# MAGIC     project_billing_type.name AS `TipoFacturacion`,
# MAGIC     
# MAGIC     empresas.Nombre AS `Empresa`,
# MAGIC     CASE WHEN empresas.Consolidada THEN 1 ELSE 0 END AS `Consolidada`, 
# MAGIC     hgz.Nombre AS `Zona`,
# MAGIC     CASE 
# MAGIC         WHEN hgr.Nombre IS NULL OR TRIM(hgr.Nombre) = '' THEN hgz.Nombre
# MAGIC         ELSE hgr.Nombre
# MAGIC     END AS `Region`,
# MAGIC     CASE 
# MAGIC         WHEN subhgr.Nombre IS NULL OR TRIM(subhgr.Nombre) = '' THEN 
# MAGIC             CASE 
# MAGIC                 WHEN hgr.Nombre IS NULL OR TRIM(hgr.Nombre) = '' THEN 
# MAGIC                     hgz.Nombre
# MAGIC                 ELSE 
# MAGIC                     hgr.Nombre
# MAGIC             END
# MAGIC         ELSE 
# MAGIC             subhgr.Nombre
# MAGIC     END AS `SubRegion`,
# MAGIC     
# MAGIC     hma.Nombre AS `HMA`,
# MAGIC     hbu.Nombre AS `HBU`,
# MAGIC     subhbu.Nombre as `SubHBU`,
# MAGIC     sector.Nombre AS `Sector`,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN Sector = 'INTERNO' and market.Nombre IS NULL THEN 'SMB'
# MAGIC         ELSE market.Nombre 
# MAGIC     END AS `Mercado`,
# MAGIC     sales.Identificador AS `Comercial`,
# MAGIC     
# MAGIC     client.id AS `IdCliente`,
# MAGIC     client_group.id AS `IdGrupoCliente`,
# MAGIC     equipostrabajo.Nombre AS `EquipoTrabajo`,
# MAGIC     
# MAGIC     CAST(DATE_FORMAT(project.start_date, 'yyyy-MM-dd') AS DATE) AS `FechaInicio`, 
# MAGIC     CAST(DATE_FORMAT(project.end_date, 'yyyy-MM-dd') AS DATE) AS `FechaFin`,
# MAGIC     
# MAGIC     CASE WHEN project.atenea_project THEN 1 ELSE 0 END AS `Atenea`,
# MAGIC     CASE WHEN project.is_billable THEN 1 ELSE 0 END AS `EsFacturable`,
# MAGIC     
# MAGIC     -- Jerarquía de los parent_projects
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject4, ': ', pp.Parent4Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject3, ': ', pp.Parent3Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject2, ': ', pp.Parent2Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject1`,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject3, ': ', pp.Parent3Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject2, ': ', pp.Parent2Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject2`,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject2, ': ', pp.Parent2Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject3`,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject4`
# MAGIC         
# MAGIC     --pp.NumeroParentProjects AS `NivelJerarquia`
# MAGIC
# MAGIC
# MAGIC FROM operaciones.silver_unhiberse.project AS project
# MAGIC
# MAGIC -- Join con ParentProjects para obtener la jerarquia
# MAGIC LEFT JOIN ParentProjects pp ON project.id = pp.ProjectID
# MAGIC
# MAGIC -- Joins originales de dim_proyectos v1
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_activity_type AS project_activity_type 
# MAGIC     ON project.project_activity_type_code = project_activity_type.code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_type AS project_type 
# MAGIC     ON project.project_type_code = project_type.code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_state AS project_state 
# MAGIC     ON project.project_state_code = project_state.code
# MAGIC
# MAGIC LEFT JOIN operaciones.silver_unhiberse.client AS client 
# MAGIC     ON project.client_id = client.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.client_group AS client_group 
# MAGIC     ON project.client_group_id = client_group.id
# MAGIC
# MAGIC LEFT JOIN operaciones.bronze_mte.empresas AS empresas
# MAGIC     ON empresas.ID = project.company_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hma AS hma
# MAGIC     ON hma.ID = project.hma_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hbu AS hbu
# MAGIC     ON hbu.ID = project.hbu_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hbu AS sector
# MAGIC     ON sector.ID = project.sector_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.mercado_auxiliar as market
# MAGIC     ON market.id_hbu = project.sector_code
# MAGIC LEFT JOIN operaciones.bronze_mte.subhbu AS subhbu
# MAGIC     ON project.sub_hbu_code = subhbu.ID
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_billing_type AS project_billing_type
# MAGIC     ON project.project_billing_type_code = project_billing_type.code
# MAGIC LEFT JOIN operaciones.bronze_mte.equipostrabajohbu AS equipostrabajo
# MAGIC     ON equipostrabajo.ID = project.work_team_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hgr AS hgr
# MAGIC     ON hgr.ID = project.region_code
# MAGIC LEFT JOIN operaciones.bronze_mte.subhgr AS subhgr
# MAGIC     ON subhgr.ID = project.sub_region_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hgz AS hgz
# MAGIC     ON hgz.ID = project.zone_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.payment_condition AS payment_condition
# MAGIC     ON project.payment_condition_id = payment_condition.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.category_market AS category_market
# MAGIC     ON category_market.code = project.market_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project AS parent_project 
# MAGIC     ON project.principal_parent_id = parent_project.id
# MAGIC LEFT JOIN operaciones.bronze_mte.salesperson AS sales
# MAGIC     ON sales.ID = project.sales_person_code
# MAGIC -- Join para secondary_parent
# MAGIC -- LEFT JOIN operaciones.silver_unhiberse.project_secondary_parent AS secondary_parent
# MAGIC --     ON project.id = secondary_parent.project_id
# MAGIC
# MAGIC WHERE project.inactive = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentarios proyectos:
# MAGIC -- mantenemos codigo de proyecto (como ID Proyecto), el proyect_ID alfanumérico no nos sirve
# MAGIC -- nombre proyecto fianl es el code + ":" name
# MAGIC -- descripcion proyecto fuera 
# MAGIC -- original_id fuera
# MAGIC -- fechas: solo yyyy-MM-dd
# MAGIC -- revisar inactivo 0/1, dar vuelta a activo = 1
# MAGIC -- condicion pago, monto total y estimado y limite no nos sirven
# MAGIC -- client_group no hace falta
# MAGIC -- nombre equipo trabajo --> equipo trabajo
# MAGIC -- proyecto padre y comentarios fuera
# MAGIC -- proyectos que vienen con barra es de atenea, si viene con guion es de unhiberse, no hace falta mayor distincion
# MAGIC -- dejamos es facturable, utimas colunas fuera
# MAGIC -- grupo sin null y sector abreviado
# MAGIC -- dates: proyecto actualizado fuera, dejamos creado, inicio y fin
# MAGIC

# COMMAND ----------

# DBTITLE 1,view_proyectos (Vista)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_proyectos AS
# MAGIC WITH ParentProjects AS (
# MAGIC     SELECT 
# MAGIC         p1.id AS ProjectID,
# MAGIC         p1.code AS ProjectCode,
# MAGIC         p1.principal_parent_id AS ParentID1,
# MAGIC         p2.code AS ParentProject1,
# MAGIC         p2.principal_parent_id AS ParentID2,
# MAGIC         p3.code AS ParentProject2,
# MAGIC         p3.principal_parent_id AS ParentID3,
# MAGIC         p4.code AS ParentProject3,
# MAGIC         p4.principal_parent_id AS ParentID4,
# MAGIC         p5.code AS ParentProject4,
# MAGIC         p5.principal_parent_id AS ParentID5,
# MAGIC         p6.code AS ParentProject5,
# MAGIC         p6.principal_parent_id AS ParentID6,
# MAGIC         p7.code AS ParentProject6,
# MAGIC         p7.principal_parent_id AS ParentID7,
# MAGIC         
# MAGIC         p2.name AS Parent1Name,
# MAGIC         p3.name AS Parent2Name,
# MAGIC         p4.name AS Parent3Name,
# MAGIC         p5.name AS Parent4Name,
# MAGIC         p6.name AS Parent5Name,
# MAGIC         p7.name AS Parent6Name,
# MAGIC         
# MAGIC         COALESCE(
# MAGIC             CASE WHEN p2.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p3.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p4.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p5.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p6.id IS NOT NULL THEN 1 ELSE 0 END
# MAGIC             + CASE WHEN p7.id IS NOT NULL THEN 1 ELSE 0 END,
# MAGIC             0
# MAGIC         ) AS NumeroParentProjects
# MAGIC     FROM operaciones.silver_unhiberse.project AS p1
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p2 ON p1.principal_parent_id = p2.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p3 ON p2.principal_parent_id = p3.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p4 ON p3.principal_parent_id = p4.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p5 ON p4.principal_parent_id = p5.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p6 ON p5.principal_parent_id = p6.id
# MAGIC     LEFT JOIN operaciones.silver_unhiberse.project p7 ON p6.principal_parent_id = p7.id
# MAGIC )
# MAGIC SELECT 
# MAGIC     -- Columnas originales de dim_proyectos v1
# MAGIC     project.code AS `IdProyecto`,
# MAGIC     INITCAP(TRIM(CONCAT(project.code, ': ', project.name))) AS `Proyecto`,
# MAGIC     --INITCAP(TRIM(CONCAT(parent_project.code, ': ', parent_project.name))) AS `ProyectoPadre`,
# MAGIC     
# MAGIC     project_activity_type.name AS `TipoActividad`,
# MAGIC     project_type.name AS `TipoProyecto`,
# MAGIC     project_state.name AS `EstadoProyecto`,
# MAGIC     
# MAGIC     CAST(DATE_FORMAT(project.created, 'yyyy-MM-dd') AS DATE) AS `FechaCreacion`,
# MAGIC     
# MAGIC     --CASE WHEN project.inactive THEN 0 ELSE 1 END AS `ProyectoActivo`,
# MAGIC     project_billing_type.name AS `TipoFacturacion`,
# MAGIC     
# MAGIC     empresas.Nombre AS `Empresa`,
# MAGIC     CASE WHEN empresas.Consolidada THEN 1 ELSE 0 END AS `Consolidada`, 
# MAGIC     hgz.Nombre AS `Zona`,
# MAGIC     CASE 
# MAGIC         WHEN hgr.Nombre IS NULL OR TRIM(hgr.Nombre) = '' THEN hgz.Nombre
# MAGIC         ELSE hgr.Nombre
# MAGIC     END AS `Region`,
# MAGIC     CASE 
# MAGIC         WHEN subhgr.Nombre IS NULL OR TRIM(subhgr.Nombre) = '' THEN 
# MAGIC             CASE 
# MAGIC                 WHEN hgr.Nombre IS NULL OR TRIM(hgr.Nombre) = '' THEN 
# MAGIC                     hgz.Nombre
# MAGIC                 ELSE 
# MAGIC                     hgr.Nombre
# MAGIC             END
# MAGIC         ELSE 
# MAGIC             subhgr.Nombre
# MAGIC     END AS `SubRegion`,
# MAGIC     
# MAGIC     hma.Nombre AS `HMA`,
# MAGIC     hbu.Nombre AS `HBU`,
# MAGIC     subhbu.Nombre as `SubHBU`,
# MAGIC     sector.Nombre AS `Sector`,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN Sector = 'INTERNO' and market.Nombre IS NULL THEN 'SMB'
# MAGIC         ELSE market.Nombre 
# MAGIC     END AS `Mercado`,
# MAGIC     sales.Identificador AS `Comercial`,
# MAGIC     
# MAGIC     --client.id AS `IdCliente`,
# MAGIC     --client_group.id AS `IdGrupoCliente`,
# MAGIC     INITCAP(TRIM(client.name)) AS `Cliente`,
# MAGIC     INITCAP(TRIM(client_group.name)) AS `GrupoCliente`,
# MAGIC     equipostrabajo.Nombre AS `EquipoTrabajo`,
# MAGIC     
# MAGIC     CAST(DATE_FORMAT(project.start_date, 'yyyy-MM-dd') AS DATE) AS `FechaInicio`, 
# MAGIC     CAST(DATE_FORMAT(project.end_date, 'yyyy-MM-dd') AS DATE) AS `FechaFin`,
# MAGIC     
# MAGIC     CASE WHEN project.atenea_project THEN 1 ELSE 0 END AS `Atenea`,
# MAGIC     CASE WHEN project.is_billable THEN 1 ELSE 0 END AS `EsFacturable`,
# MAGIC     
# MAGIC     -- Jerarquía de los parent_projects
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject4, ': ', pp.Parent4Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject3, ': ', pp.Parent3Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject2, ': ', pp.Parent2Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject1`,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject3, ': ', pp.Parent3Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject2, ': ', pp.Parent2Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject2`,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject2, ': ', pp.Parent2Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject3`,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN pp.NumeroParentProjects >= 4 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name))
# MAGIC         WHEN pp.NumeroParentProjects >= 3 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         WHEN pp.NumeroParentProjects >= 2 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         WHEN pp.NumeroParentProjects >= 1 THEN INITCAP(CONCAT(pp.ParentProject1, ': ', pp.Parent1Name)) -- Repetir ParentProject1
# MAGIC         ELSE INITCAP(CONCAT(project.code, ': ', project.name)) -- El proyecto si no hay parents
# MAGIC     END AS `ParentProject4`
# MAGIC         
# MAGIC     --pp.NumeroParentProjects AS `NivelJerarquia`
# MAGIC
# MAGIC
# MAGIC FROM operaciones.silver_unhiberse.project AS project
# MAGIC
# MAGIC -- Join con ParentProjects para obtener la jerarquia
# MAGIC LEFT JOIN ParentProjects pp ON project.id = pp.ProjectID
# MAGIC
# MAGIC -- Joins originales de dim_proyectos v1
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_activity_type AS project_activity_type 
# MAGIC     ON project.project_activity_type_code = project_activity_type.code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_type AS project_type 
# MAGIC     ON project.project_type_code = project_type.code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_state AS project_state 
# MAGIC     ON project.project_state_code = project_state.code
# MAGIC
# MAGIC LEFT JOIN operaciones.silver_unhiberse.client AS client 
# MAGIC     ON project.client_id = client.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.client_group AS client_group 
# MAGIC     ON project.client_group_id = client_group.id
# MAGIC
# MAGIC LEFT JOIN operaciones.bronze_mte.empresas AS empresas
# MAGIC     ON empresas.ID = project.company_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hma AS hma
# MAGIC     ON hma.ID = project.hma_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hbu AS hbu
# MAGIC     ON hbu.ID = project.hbu_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hbu AS sector
# MAGIC     ON sector.ID = project.sector_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.mercado_auxiliar as market
# MAGIC     ON market.id_hbu = project.sector_code
# MAGIC LEFT JOIN operaciones.bronze_mte.subhbu AS subhbu
# MAGIC     ON project.sub_hbu_code = subhbu.ID
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project_billing_type AS project_billing_type
# MAGIC     ON project.project_billing_type_code = project_billing_type.code
# MAGIC LEFT JOIN operaciones.bronze_mte.equipostrabajohbu AS equipostrabajo
# MAGIC     ON equipostrabajo.ID = project.work_team_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hgr AS hgr
# MAGIC     ON hgr.ID = project.region_code
# MAGIC LEFT JOIN operaciones.bronze_mte.subhgr AS subhgr
# MAGIC     ON subhgr.ID = project.sub_region_code
# MAGIC LEFT JOIN operaciones.bronze_mte.hgz AS hgz
# MAGIC     ON hgz.ID = project.zone_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.payment_condition AS payment_condition
# MAGIC     ON project.payment_condition_id = payment_condition.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.category_market AS category_market
# MAGIC     ON category_market.code = project.market_code
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project AS parent_project 
# MAGIC     ON project.principal_parent_id = parent_project.id
# MAGIC LEFT JOIN operaciones.bronze_mte.salesperson AS sales
# MAGIC     ON sales.ID = project.sales_person_code
# MAGIC -- Join para secondary_parent
# MAGIC -- LEFT JOIN operaciones.silver_unhiberse.project_secondary_parent AS secondary_parent
# MAGIC --     ON project.id = secondary_parent.project_id
# MAGIC
# MAGIC WHERE project.inactive = false;

# COMMAND ----------

# DBTITLE 1,Facturas (Fact)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW fact_facturas AS
# MAGIC SELECT 
# MAGIC     -- invoice columns
# MAGIC     i.code AS CodFactura,  -- Invoice code
# MAGIC     p.code AS IdProyecto,
# MAGIC     --CONCAT(p.code, ': ', p.name) AS Proyecto,  -- Concatenating project code and name
# MAGIC     --CASE WHEN i.inactive THEN 0 ELSE 1 END AS Activo,  -- Active flag for the invoice
# MAGIC     --CAST(DATE_FORMAT(i.date, 'yyyy-MM-dd') AS DATE) AS FechaFactura,  -- Date of the invoice
# MAGIC     --i.attention_to AS AtencionA,  -- Campo llamado attention to 
# MAGIC     --i.comments AS Comentarios,  -- Comentarios del invoice
# MAGIC     --pe.name AS Periodo,  -- join Period ID
# MAGIC     i.invoice_number AS Documento,  -- Invoice number
# MAGIC     CASE WHEN i.charged THEN 1 ELSE 0 END AS Facturado,  -- Flag para invoice esta facturado
# MAGIC     --CASE WHEN i.overdue THEN 1 ELSE 0 END AS Vencido,  -- Flag para cuando invoice esta vencido/pasado de fecha
# MAGIC     -- invoice_item-specific columns
# MAGIC     ii.order_number AS NumeroOrden,  -- Order number for the item
# MAGIC     ii.concept AS Concepto,  -- Concept of the item
# MAGIC     ROUND(ii.item_amount, 2) AS ImpFacturado,  -- Amount of the invoice item
# MAGIC
# MAGIC     --CAST(ii.item_count AS int) AS CantidadItems, --chequear si se va a utilizar 
# MAGIC
# MAGIC     --CAST(DATE_FORMAT(ii.created, 'yyyy-MM-dd') AS DATE) AS FechaCreacionItem,  -- Creation timestamp for the item
# MAGIC     --CAST(DATE_FORMAT(ii.updated, 'yyyy-MM-dd') AS DATE) AS FechaActualizacionItem,  -- Update timestamp for the item
# MAGIC     -- Join category y state 
# MAGIC     ict.name AS Categoria,  -- Category nombre (de invoice_category_type)
# MAGIC     ist.name AS EstadoFactura,  -- State nombre (de invoice_state_type)
# MAGIC     CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
# MAGIC
# MAGIC FROM operaciones.silver_unhiberse.invoice_item ii
# MAGIC LEFT JOIN operaciones.silver_unhiberse.invoice i ON ii.invoice_id = i.id  -- Join to get invoice details
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project p ON i.project_id = p.id  -- Join to get project details
# MAGIC LEFT JOIN operaciones.silver_unhiberse.invoice_category_type ict ON ii.category_type_code = ict.code  -- Join to get category type name
# MAGIC LEFT JOIN operaciones.silver_unhiberse.invoice_state_type ist ON i.state_code = ist.code  -- Join to get invoice state name
# MAGIC LEFT JOIN operaciones.silver_unhiberse.period pe ON i.period_id = pe.id
# MAGIC WHERE ii.inactive = false
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentarios facturas: 
# MAGIC -- facturas id nos lo quitamos
# MAGIC -- fecha solo yyy-MM-dd
# MAGIC -- nombre proyecto -->idPoryecto al codigo
# MAGIC -- comentarios factura fuera
# MAGIC -- Numero Factura viene como invoice_number--> Documento
# MAGIC -- Codigo_factura vienen como invoice_code,tiene que ser el ID corto de la factura, y no el alfanumerico. Este invoice_code no esta en la linea (invoice_item)
# MAGIC -- invoice_code para hacer la combi
# MAGIC -- proyecto padre, periodo, creado, actualizado por, archivo factura fuera
# MAGIC -- concepto mandatory mantener
# MAGIC -- invoice_category_type sirve un join
# MAGIC -- item count es "Cantidad", podemos pasar a numero entero porque no hay medios objetos 
# MAGIC -- Item amount "Importe"
# MAGIC -- fechas crated y updated poner en el formato correcto
# MAGIC -- inactive poner al false true a 0/1 y giramos a activo

# COMMAND ----------

# DBTITLE 1,Incurridos (Fact)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW fact_incurridos AS
# MAGIC SELECT 
# MAGIC     -- incurrence-level info
# MAGIC     i.code AS Documento,  -- Incurrence code
# MAGIC     p.code AS IdProyecto,  --Project code
# MAGIC     --CONCAT(p.code, ': ', p.name) AS Proyecto,  -- Concatenating project code and name
# MAGIC     --CASE WHEN i.inactive THEN 0 ELSE 1 END AS Activo,  -- Active flag incurrence, girado para mostrar activos con 1
# MAGIC     --CAST(DATE_FORMAT(i.date, 'yyyy-MM-dd') AS DATE) AS Fecha,  -- Date of the incurrence (si se quiere dd-MM-yyyy se pierde el tipo fecha y pasa a tipo string)
# MAGIC     --pe.name AS Periodo,  -- Period name
# MAGIC     -- incurrence_item-specific columns
# MAGIC     --ii.order_number AS NumeroOrden,  -- Order number
# MAGIC     ii.concept AS Concepto,  -- Concept
# MAGIC     ROUND(ii.amount, 2) AS ImpIncurrido,  -- Amount of the item
# MAGIC     --CASE WHEN ii.inactive THEN 0 ELSE 1 END AS ItemActivo,  -- Active flag for the incurrence item, ya tenemos la de incurrence
# MAGIC     --CAST(DATE_FORMAT(ii.created, 'yyyy-MM-dd') AS DATE) AS Fecha  -- No sirve porque si hay una migracion y se actualiza se puede mover, vamos a usar la fecha de inicio asociada al periodo
# MAGIC     CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
# MAGIC
# MAGIC     --CAST(DATE_FORMAT(ii.updated, 'yyyy-MM-dd') AS DATE) AS FechaActualizacionItem  -- Update timestamp for the item
# MAGIC     --ii.incurrence_id AS IncurrenceId  -- Link back to incurrence
# MAGIC
# MAGIC
# MAGIC FROM operaciones.silver_unhiberse.incurrence_item ii
# MAGIC LEFT JOIN operaciones.silver_unhiberse.incurrence i ON ii.incurrence_id = i.id  -- Join to get incurrence details
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project p ON i.project_id = p.id  -- Join to get project details
# MAGIC LEFT JOIN operaciones.silver_unhiberse.period pe ON i.period_id = pe.id
# MAGIC
# MAGIC WHERE  ii.inactive = false
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentarios incurridos: 
# MAGIC -- estado y fechas (quitamos incurrido)
# MAGIC -- comentario fuera
# MAGIC -- girrar inactivo a 0/1 activo
# MAGIC -- codigo incurrido la cambiamos a "Documento"
# MAGIC -- proyecto padre hacia la dcha nos lo cargamos
# MAGIC -- amount lo nombramos Factura
# MAGIC -- inactive esta ya en la cabecera, nos lo usamos el de item
# MAGIC -- order_number si

# COMMAND ----------

# DBTITLE 1,Gastos (Fact)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW fact_gastos AS
# MAGIC SELECT 
# MAGIC     --expense.id AS `ID`
# MAGIC     project.code AS `IdProyecto`,  -- Nombre del proyecto relacionado
# MAGIC     --period.name AS `Periodo`, --hacer join con period, lo quitamos por el momento
# MAGIC     expense_type.name AS `Tipo`, --join expense type
# MAGIC     expense.section AS `Seccion`, --correcto
# MAGIC
# MAGIC     --expense.provider AS `Proveedor`, --correcto
# MAGIC     --expense.provider_cif AS `CIF_Proveedor`, -- cif no sirve
# MAGIC     
# MAGIC     CASE
# MAGIC         WHEN expense.provider IS NULL THEN '#N/D'
# MAGIC         ELSE INITCAP(expense.provider) --utilizan #N/D para indicar que no hay datos (cuando sea que lo usan)
# MAGIC     END AS `Proveedor`,
# MAGIC     CASE
# MAGIC         WHEN expense.provider_cif IS NULL THEN '#N/D'
# MAGIC         ELSE expense.provider_cif
# MAGIC     END AS `CIFProveedor`,
# MAGIC     /*
# MAGIC     Se han formateado los nulls para mostrar #N/D, que es el valor que utilizaban cuando no se tenía dato. Cuando se tenga acceso a la tabla de proveedores cruzar datos para evitar hacer este arreglo.
# MAGIC     */
# MAGIC     
# MAGIC     ROUND(expense.amount, 2) AS `ImpGastado`, --ta bein, hay 38 rows que llegan hasta 7 decimales
# MAGIC     CAST(CONCAT(SUBSTRING(pe.code, 1, 4), '-', SUBSTRING(pe.code, 6, 2), '-01') AS DATE) AS Fecha
# MAGIC     --CAST(DATE_FORMAT(expense.created, 'yyyy-MM-dd') AS DATE) AS `FechaCreado`,  --checkear si queremos este timestamp
# MAGIC     --CAST(DATE_FORMAT(expense.updated, 'yyyy-MM-dd') AS DATE) AS `FechaActualizado` --checkear si queremos este timestamp
# MAGIC     --CASE WHEN expense.inactive THEN 0 ELSE 1 END AS `Activo`  -- esta girado para comodidad
# MAGIC FROM operaciones.silver_unhiberse.expense AS expense
# MAGIC LEFT JOIN operaciones.silver_unhiberse.project AS project 
# MAGIC     ON expense.project_id = project.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.period AS pe
# MAGIC     ON expense.period_id = pe.id
# MAGIC LEFT JOIN operaciones.silver_unhiberse.expense_type AS expense_type
# MAGIC     ON expense.expense_type_code = expense_type.code
# MAGIC
# MAGIC WHERE expense.inactive = false
# MAGIC ORDER BY project.name;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentarios expenses: 
# MAGIC -- quitamos gastos_id
# MAGIC -- mantenemos project_id
# MAGIC -- checkear periodo, la quitamos por el momento
# MAGIC -- seccion_gasto esta codificado a medias como struct cuando lo traen a unhiberse y queda con/sin numeros
# MAGIC -- cambiar formato montosde decimal(38,18) a 2 decimales
# MAGIC -- formatear a que venga yyyy-MM-dd, pero que quede como un valor date, y no string. 
# MAGIC -- activo/inactivo por girarlo comodidad a la hora de leerlo/llamarlo
# MAGIC -- Si seccion es Personal, el proveedor/CIFproveedor es null, configuramos #N/D
# MAGIC -- la tabla de proveedores no la tenemos para poder cruzarla
# MAGIC -- podemos dejar Nombre Proveedor y CIF al lado

# COMMAND ----------

# DBTITLE 1,CASE WHEN para decimales expenses amount
# MAGIC %sql
# MAGIC /*
# MAGIC CASE
# MAGIC     -- Check if there are non-zero digits up to the 7th decimal place
# MAGIC     WHEN ROUND(expense.amount, 7) LIKE '%._______00%' AND ROUND(expense.amount, 7) NOT LIKE '%.______00%' THEN
# MAGIC         CAST(ROUND(expense.amount, 7) AS DECIMAL(24,7))
# MAGIC
# MAGIC     -- Check if there are non-zero digits up to the 6th decimal place
# MAGIC     WHEN ROUND(expense.amount, 7) LIKE '%.______00%' AND ROUND(expense.amount, 7) NOT LIKE '%._____00%' THEN
# MAGIC         CAST(ROUND(expense.amount, 6) AS DECIMAL(24,6))
# MAGIC
# MAGIC     -- Check if there are non-zero digits up to the 5th decimal place
# MAGIC     WHEN ROUND(expense.amount, 7) LIKE '%._____00%' AND ROUND(expense.amount, 7) NOT LIKE '%.____00%' THEN
# MAGIC         CAST(ROUND(expense.amount, 5) AS DECIMAL(24,5))
# MAGIC
# MAGIC     -- Check if there are non-zero digits up to the 4th decimal place
# MAGIC     WHEN ROUND(expense.amount, 7) LIKE '%.____00%' AND ROUND(expense.amount, 7) NOT LIKE '%.___00%' THEN
# MAGIC         CAST(ROUND(expense.amount, 4) AS DECIMAL(24,4))
# MAGIC
# MAGIC     -- Check if there are non-zero digits up to the 3rd decimal place
# MAGIC     WHEN ROUND(expense.amount, 7) LIKE '%.___00%' AND ROUND(expense.amount, 7) NOT LIKE '%.__00%' THEN
# MAGIC         CAST(ROUND(expense.amount, 3) AS DECIMAL(24,3))
# MAGIC
# MAGIC     -- Check if there are non-zero digits up to the 2nd decimal place
# MAGIC     WHEN ROUND(expense.amount, 7) LIKE '%.__00%' AND ROUND(expense.amount, 7) NOT LIKE '%.0%' THEN
# MAGIC         CAST(ROUND(expense.amount, 2) AS DECIMAL(24,2))
# MAGIC
# MAGIC     -- Fallback to 2 decimal places in case no other condition is met
# MAGIC     ELSE
# MAGIC         CAST(ROUND(expense.amount, 2) AS DECIMAL(24,2))
# MAGIC END AS `MontoPreciso`
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentarios generales: 
# MAGIC -- Para sector y mercado, coger de hbu/hma, porque hbu y sector comparten el Id, y hbu y hma cruzan los datos de la categorias mercado
# MAGIC -- Para nombres quitar _, pasamos a mayus primera, lower case resto
# MAGIC -- Para todo traemos nombres, dejamos todo lo de la linea, y sumamos los necesarios para la cabecera.

# COMMAND ----------

# DBTITLE 1,GrupoCliente (dim)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dim_grupocliente AS
# MAGIC SELECT 
# MAGIC   cg.id AS `IdGrupoCliente`, 
# MAGIC   INITCAP(TRIM(cg.name)) AS `GrupoCliente`
# MAGIC
# MAGIC FROM operaciones.silver_unhiberse.client_group AS cg
# MAGIC

# COMMAND ----------

# DBTITLE 1,datetime
proyectos_df = spark.sql("SELECT DISTINCT * FROM dim_proyectos")
clientes_df = spark.sql("SELECT DISTINCT * FROM dim_clientes")
grupocliente_df = spark.sql("SELECT DISTINCT * FROM dim_grupocliente")
facturas_df = spark.sql("SELECT DISTINCT * FROM fact_facturas")
gastos_df = spark.sql("SELECT DISTINCT * FROM fact_gastos")
incurridos_df = spark.sql("SELECT DISTINCT * FROM fact_incurridos")
view_proyectos_df = spark.sql("SELECT DISTINCT * FROM view_proyectos")

proyectos_df_datetime = proyectos_df.withColumn("FechaCarga", F.current_timestamp())
clientes_df_datetime = clientes_df.withColumn("FechaCarga", F.current_timestamp())
grupocliente_df_datetime = grupocliente_df.withColumn("FechaCarga", F.current_timestamp())
facturas_df_datetime = facturas_df.withColumn("FechaCarga", F.current_timestamp())
gastos_df_datetime = gastos_df.withColumn("FechaCarga", F.current_timestamp())
incurridos_df_datetime = incurridos_df.withColumn("FechaCarga", F.current_timestamp())
view_proyectos_df_datetime = view_proyectos_df.withColumn("FechaCarga", F.current_timestamp())

# COMMAND ----------

# Vista
proyectos_df_datetime.createOrReplaceTempView("dim_proyectos")
clientes_df_datetime.createOrReplaceTempView("dim_clientes")
grupocliente_df_datetime.createOrReplaceTempView("dim_grupocliente")
facturas_df_datetime.createOrReplaceTempView("fact_facturas")
gastos_df_datetime.createOrReplaceTempView("fact_gastos")
incurridos_df_datetime.createOrReplaceTempView("fact_incurridos")
view_proyectos_df_datetime.createOrReplaceTempView("view_proyectos")

# Guardar como Delta table
spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.dim_proyecto
    AS SELECT DISTINCT * FROM dim_proyectos
    """)

spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.dim_cliente
    AS SELECT DISTINCT * FROM dim_clientes
    """)

spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.dim_grupocliente
    AS SELECT DISTINCT * FROM dim_grupocliente
    """)

spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.fact_facturas
    AS SELECT DISTINCT * FROM fact_facturas
    """)

spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.fact_gastos
    AS SELECT DISTINCT * FROM fact_gastos
    """)

spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.fact_incurridos
    AS SELECT DISTINCT * FROM fact_incurridos
    """)

spark.sql("""
    CREATE OR REPLACE TABLE operaciones.gold_unhiberse.view_proyectos
    AS SELECT DISTINCT * FROM view_proyectos
    """)
