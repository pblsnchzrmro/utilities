# Databricks notebook source
import pyodbc
import pandas as pd
import time
import sys
import numpy as np
import socket

# COMMAND ----------

# DBTITLE 1,table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW tabla_matias AS
# MAGIC WITH ResponsableUsers AS (
# MAGIC     SELECT 
# MAGIC         cast(u.ID as INTEGER) as IdUsuario,
# MAGIC         trim(u.EmailCorporativo) as EmailResponsable
# MAGIC     FROM operaciones.bronze_mte.fact_usuarios u
# MAGIC     WHERE u.EmailCorporativo IS NOT NULL AND trim(u.EmailCorporativo) != ''
# MAGIC ),
# MAGIC SplitResponsableshBU AS (
# MAGIC     SELECT 
# MAGIC     hbu.IdhBU,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_hbu hbu
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hbu.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hbu.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadoshBU AS (
# MAGIC     SELECT 
# MAGIC         sr.IdhBU,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsableshBU sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdhBU
# MAGIC ),
# MAGIC SplitResponsableshMA AS (
# MAGIC     SELECT 
# MAGIC     hma.IdhMA,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_hma hma
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hma.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hma.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadoshMA AS (
# MAGIC     SELECT 
# MAGIC         sr.IdhMA,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsableshMA sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdhMA
# MAGIC ),
# MAGIC SplitResponsablesEquipoMultiarea AS (
# MAGIC     SELECT 
# MAGIC     etm.IdEquipoMultitarea,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_equipostrabajomultiarea etm
# MAGIC     LATERAL VIEW posexplode(split(coalesce(etm.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(etm.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadosEquipoMultiarea AS (
# MAGIC     SELECT 
# MAGIC         sr.IdEquipoMultitarea,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsablesEquipoMultiarea sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdEquipoMultitarea
# MAGIC ), 
# MAGIC SplitResponsablesSubhBU AS (
# MAGIC     SELECT 
# MAGIC     shbu.IdSubhBU,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_subhbu shbu
# MAGIC     LATERAL VIEW posexplode(split(coalesce(shbu.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(shbu.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadosSubhBU AS (
# MAGIC     SELECT 
# MAGIC         sr.IdSubhBU,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsablesSubhBU sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdSubhBU
# MAGIC ),
# MAGIC SplitResponsableshGZ AS (
# MAGIC     SELECT 
# MAGIC     hgz.IdhGZ,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_hgz hgz
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hgz.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hgz.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadoshGZ AS (
# MAGIC     SELECT 
# MAGIC         sr.IdhGZ,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsableshGZ sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdhGZ
# MAGIC ),
# MAGIC SplitResponsableshGR AS (
# MAGIC     SELECT 
# MAGIC     hgr.IdhGR,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_hgr hgr
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hgr.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(hgr.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadoshGR AS (
# MAGIC     SELECT 
# MAGIC         sr.IdhGR,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsableshGR sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdhGR
# MAGIC ),
# MAGIC SplitResponsablesSubhGR AS (
# MAGIC     SELECT 
# MAGIC     shgr.IdSubhGR,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_subhgr shgr
# MAGIC     LATERAL VIEW posexplode(split(coalesce(shgr.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(shgr.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadosSubhGR AS (
# MAGIC     SELECT 
# MAGIC         sr.IdSubhGR,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsablesSubhGR sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdSubhGR
# MAGIC ), 
# MAGIC SplitResponsablesEquipoHBU AS (
# MAGIC     SELECT 
# MAGIC     ethbu.IdEquipohBU,
# MAGIC     trim(name_value) as ResponsableName,
# MAGIC     trim(email_value) as EmailResponsable
# MAGIC     FROM operaciones.gold_mte.dim_equipostrabajohbu ethbu
# MAGIC     LATERAL VIEW posexplode(split(coalesce(ethbu.Responsable, ''), ',')) as pos, name_value
# MAGIC     LATERAL VIEW posexplode(split(coalesce(ethbu.ResponsableEmail, ''), ',')) as pos_email, email_value
# MAGIC     WHERE pos = pos_email
# MAGIC     AND trim(name_value) != '' AND trim(email_value) != ''
# MAGIC ), 
# MAGIC ResponsablesAgregadosEquipoHBU AS (
# MAGIC     SELECT 
# MAGIC         sr.IdEquipohBU,
# MAGIC         concat_ws(', ', collect_set(cast(ru.IdUsuario as string))) as ResponsableIDs
# MAGIC     FROM SplitResponsablesEquipoHBU sr
# MAGIC     LEFT JOIN ResponsableUsers ru ON sr.EmailResponsable = ru.EmailResponsable
# MAGIC     GROUP BY sr.IdEquipohBU
# MAGIC ),
# MAGIC
# MAGIC aux_approver_id_map AS (
# MAGIC   WITH 
# MAGIC   source_names AS (
# MAGIC     SELECT DISTINCT
# MAGIC       AprobadorVacaciones AS approver_name,
# MAGIC       LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   AprobadorVacaciones,
# MAGIC                   'á', 'a'),
# MAGIC                 'é', 'e'),
# MAGIC               'í', 'i'),
# MAGIC             'ó', 'o'),
# MAGIC           'ú', 'u')
# MAGIC       )) AS standardized_name
# MAGIC     FROM people.gold_mteia.usuarios_ia
# MAGIC     WHERE AprobadorVacaciones IS NOT NULL
# MAGIC   ),
# MAGIC   user_ids AS (
# MAGIC     SELECT
# MAGIC       ID,
# MAGIC       NombreCompleto AS original_name,
# MAGIC       LOWER(TRIM(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               REGEXP_REPLACE(
# MAGIC                 REGEXP_REPLACE(
# MAGIC                   CONCAT(
# MAGIC                     TRIM(SPLIT(NombreCompleto, ',')[1]), 
# MAGIC                     ' ',
# MAGIC                     TRIM(SPLIT(NombreCompleto, ',')[0])
# MAGIC                   ),
# MAGIC                   'á', 'a'),
# MAGIC                 'é', 'e'),
# MAGIC               'í', 'i'),
# MAGIC             'ó', 'o'),
# MAGIC           'ú', 'u')
# MAGIC       )) AS standardized_name,
# MAGIC       EmailCorporativo
# MAGIC     FROM operaciones.bronze_mte.fact_usuarios
# MAGIC     WHERE NombreCompleto IS NOT NULL
# MAGIC   ),
# MAGIC   
# MAGIC   exact_matches AS (
# MAGIC     SELECT 
# MAGIC       s.approver_name AS AprobadorVacaciones,
# MAGIC       u.ID AS AprobadorID,
# MAGIC       u.EmailCorporativo as AprobadorEmail
# MAGIC     FROM source_names s
# MAGIC     JOIN user_ids u ON s.standardized_name = u.standardized_name
# MAGIC   ),
# MAGIC   
# MAGIC   partial_matches AS (
# MAGIC     SELECT 
# MAGIC       'Carlos Martinez Lopez' AS AprobadorVacaciones,
# MAGIC       1012 AS AprobadorID,
# MAGIC       (SELECT EmailCorporativo FROM user_ids WHERE ID = 1012) as AprobadorEmail,
# MAGIC       0 as match_type  
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC       s.approver_name AS AprobadorVacaciones,
# MAGIC       u.ID AS AprobadorID,
# MAGIC       u.EmailCorporativo as AprobadorEmail,
# MAGIC       1 as match_type
# MAGIC     FROM source_names s
# MAGIC     JOIN user_ids u ON 
# MAGIC       u.standardized_name LIKE CONCAT('%', s.standardized_name, '%')
# MAGIC       AND s.approver_name NOT IN (SELECT AprobadorVacaciones FROM exact_matches)
# MAGIC       AND s.approver_name != 'Carlos Martinez Lopez'  
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC       s.approver_name AS AprobadorVacaciones,
# MAGIC       u.ID AS AprobadorID,
# MAGIC       u.EmailCorporativo as AprobadorEmail,
# MAGIC       2 as match_type
# MAGIC     FROM source_names s
# MAGIC     JOIN user_ids u ON 
# MAGIC       s.standardized_name LIKE CONCAT('%', u.standardized_name, '%')
# MAGIC       AND s.approver_name NOT IN (SELECT AprobadorVacaciones FROM exact_matches)
# MAGIC       AND s.approver_name != 'Carlos Martinez Lopez' 
# MAGIC   ),
# MAGIC
# MAGIC   
# MAGIC   token_matches AS (
# MAGIC     SELECT 
# MAGIC       s.approver_name AS AprobadorVacaciones,
# MAGIC       u.ID AS AprobadorID,
# MAGIC       u.EmailCorporativo as AprobadorEmail
# MAGIC     FROM source_names s
# MAGIC     CROSS JOIN user_ids u
# MAGIC     WHERE
# MAGIC       array_contains(split(u.standardized_name, ' '), split(s.standardized_name, ' ')[0])
# MAGIC       AND array_contains(split(u.standardized_name, ' '), element_at(split(s.standardized_name, ' '), size(split(s.standardized_name, ' '))))
# MAGIC       AND s.approver_name NOT IN (
# MAGIC         SELECT AprobadorVacaciones FROM exact_matches
# MAGIC         UNION
# MAGIC         SELECT AprobadorVacaciones FROM partial_matches
# MAGIC       )
# MAGIC   )
# MAGIC   
# MAGIC   SELECT * FROM exact_matches
# MAGIC   UNION ALL
# MAGIC   SELECT AprobadorVacaciones, AprobadorID, AprobadorEmail 
# MAGIC   FROM (
# MAGIC     SELECT *, row_number() OVER (PARTITION BY AprobadorVacaciones ORDER BY match_type) as rn 
# MAGIC     FROM partial_matches
# MAGIC   ) WHERE rn = 1
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM token_matches
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     cast(u.ID as INTEGER) as IdUsuario,
# MAGIC     concat_ws(' ', split(initcap(trim(u.NombreCompleto)), ',')[1], split(initcap(trim(u.NombreCompleto)), ',')[0]) as NombreCompleto,
# MAGIC     split(initcap(trim(u.NombreCompleto)), ',')[1] as Nombre,
# MAGIC     split(initcap(trim(u.NombreCompleto)), ',')[0] as Apellidos,
# MAGIC     u.EmailCorporativo, 
# MAGIC     u.IdCentroTrabajo, 
# MAGIC     ct.CentroTrabajo, 
# MAGIC     uia.Direccion, 
# MAGIC     u.IdEmpresa, 
# MAGIC     e.Empresa, 
# MAGIC     u.IdEquipoTrabajoMultiarea, 
# MAGIC     etm.EquipoMultitarea, 
# MAGIC     u.IdEquipoTrabajohBU, 
# MAGIC     ethbu.EquipoHBU as EquipoTrabajohBU, 
# MAGIC     cast(u.FechaAlta as Date) as FechaAlta, 
# MAGIC     cast(u.FechaBaja as Date) as FechaBaja, 
# MAGIC     cast(u.FechaAntiguedad as date) as FechaAntiguedad, 
# MAGIC     u.IdGrupoNivel, 
# MAGIC     gn.GrupoNivel,
# MAGIC     u.IdCarreraProfesional, 
# MAGIC     cp.CarreraProfesional,
# MAGIC     u.IdNivelProfesional, 
# MAGIC     np.NivelProfesional,
# MAGIC     u.PuestoTrabajo, 
# MAGIC     u.Recruiter, 
# MAGIC     u.IdSubhBU, 
# MAGIC     shbu.SubhBU,
# MAGIC     u.IdSubhGR,
# MAGIC     shgr.SubhGR, 
# MAGIC     u.TipoPerfil, 
# MAGIC     u.IdTitulacion, 
# MAGIC     u.Titulacion, 
# MAGIC     u.Usuario,
# MAGIC     u.IdComunidadesTecnologicas,
# MAGIC     u.ComunidadesTecnologicas,
# MAGIC     u.IdhBU, 
# MAGIC     hbu.hBU,
# MAGIC     u.IdhGR, 
# MAGIC     hgr.hGR,
# MAGIC     u.IdhGZ, 
# MAGIC     hgz.hGZ,
# MAGIC     u.IdhMA, 
# MAGIC     hma.hMA,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN u.IdhBU IS NULL THEN NULL
# MAGIC         WHEN ra.ResponsableIDs IS NULL OR trim(ra.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE ra.ResponsableIDs
# MAGIC     END as IdResponsablehBU,
# MAGIC     CASE
# MAGIC         WHEN u.IdhBU IS NULL THEN NULL
# MAGIC         ELSE hbu.Responsable
# MAGIC     END as ResponsablehBU,
# MAGIC     CASE
# MAGIC         WHEN u.IdhBU IS NULL THEN NULL
# MAGIC         ELSE hbu.ResponsableEmail
# MAGIC     END as ResponsableEmailhBU,
# MAGIC     
# MAGIC     CASE 
# MAGIC         WHEN u.IdhMA IS NULL THEN NULL
# MAGIC         WHEN rah.ResponsableIDs IS NULL OR trim(rah.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE rah.ResponsableIDs
# MAGIC     END as IdResponsablehMA,
# MAGIC     CASE
# MAGIC         WHEN u.IdhMA IS NULL THEN NULL
# MAGIC         ELSE hma.Responsable
# MAGIC     END as ResponsablehMA,
# MAGIC     CASE
# MAGIC         WHEN u.IdhMA IS NULL THEN NULL
# MAGIC         ELSE hma.ResponsableEmail
# MAGIC     END as ResponsableEmailhMA,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN u.IdEquipoTrabajohBU IS NULL THEN NULL
# MAGIC         WHEN raeh.ResponsableIDs IS NULL OR trim(raeh.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE raeh.ResponsableIDs
# MAGIC     END as IdResponsableEquipoTrabajohBU,
# MAGIC     CASE
# MAGIC         WHEN u.IdEquipoTrabajohBU IS NULL THEN NULL
# MAGIC         ELSE ethbu.Responsable
# MAGIC     END as ResponsableEquipoTrabajohBU,
# MAGIC     CASE
# MAGIC         WHEN u.IdEquipoTrabajohBU IS NULL THEN NULL
# MAGIC         ELSE ethbu.ResponsableEmail
# MAGIC     END as ResponsableEmailEquipoTrabajohBU,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN u.IdEquipoTrabajoMultiarea IS NULL THEN NULL
# MAGIC         WHEN ram.ResponsableIDs IS NULL OR trim(ram.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE ram.ResponsableIDs
# MAGIC     END as IdResponsableEquipoTrabajoMultiarea,
# MAGIC     CASE
# MAGIC         WHEN u.IdEquipoTrabajoMultiarea IS NULL THEN NULL
# MAGIC         ELSE etm.Responsable
# MAGIC     END as ResponsableEquipoTrabajoMultiarea,
# MAGIC     CASE
# MAGIC         WHEN u.IdEquipoTrabajoMultiarea IS NULL THEN NULL
# MAGIC         ELSE etm.ResponsableEmail
# MAGIC     END as ResponsableEmailEquipoTrabajoMultiarea,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN u.IdSubhBU IS NULL THEN NULL
# MAGIC         WHEN rasb.ResponsableIDs IS NULL OR trim(rasb.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE rasb.ResponsableIDs
# MAGIC     END as IdResponsableSubhBU,
# MAGIC     CASE
# MAGIC         WHEN u.IdSubhBU IS NULL THEN NULL
# MAGIC         ELSE shbu.Responsable
# MAGIC     END as ResponsableSubhBU,
# MAGIC     CASE
# MAGIC         WHEN u.IdSubhBU IS NULL THEN NULL
# MAGIC         ELSE shbu.ResponsableEmail
# MAGIC     END as ResponsableEmailSubhBU,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN u.IdhGZ IS NULL THEN NULL
# MAGIC         WHEN rahgz.ResponsableIDs IS NULL OR trim(rahgz.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE rahgz.ResponsableIDs
# MAGIC     END as IdResponsablehGZ,
# MAGIC     CASE
# MAGIC         WHEN u.IdhGZ IS NULL THEN NULL
# MAGIC         ELSE hgz.Responsable
# MAGIC     END as ResponsablehGZ,
# MAGIC     CASE
# MAGIC         WHEN u.IdhGZ IS NULL THEN NULL
# MAGIC         ELSE hgz.ResponsableEmail
# MAGIC     END as ResponsableEmailhGZ,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN u.IdhGR IS NULL THEN NULL
# MAGIC         WHEN rahgr.ResponsableIDs IS NULL OR trim(rahgr.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE rahgr.ResponsableIDs
# MAGIC     END as IdResponsablehGR,
# MAGIC     CASE
# MAGIC         WHEN u.IdhGR IS NULL THEN NULL
# MAGIC         ELSE hgr.Responsable
# MAGIC     END as ResponsablehGR,
# MAGIC     CASE
# MAGIC         WHEN u.IdhGR IS NULL THEN NULL
# MAGIC         ELSE hgr.ResponsableEmail
# MAGIC     END as ResponsableEmailhGR,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN u.IdSubhGR IS NULL THEN NULL
# MAGIC         WHEN rasgr.ResponsableIDs IS NULL OR trim(rasgr.ResponsableIDs) = '' THEN NULL
# MAGIC         ELSE rasgr.ResponsableIDs
# MAGIC     END as IdResponsableSubhGR,
# MAGIC     CASE
# MAGIC         WHEN u.IdSubhGR IS NULL THEN NULL
# MAGIC         ELSE shgr.Responsable
# MAGIC     END as ResponsableSubhGR,
# MAGIC     CASE
# MAGIC         WHEN u.IdSubhGR IS NULL THEN NULL
# MAGIC         ELSE shgr.ResponsableEmail
# MAGIC     END as ResponsableEmailSubhGR,
# MAGIC
# MAGIC     map.AprobadorID AS IdAprobadorVacaciones,
# MAGIC     uia.AprobadorVacaciones, 
# MAGIC     map.AprobadorEmail AS AprobadorVacacionesEmail,
# MAGIC     fu.`AñosAntiguedad`, 
# MAGIC     fu.RangoAntiguedad, 
# MAGIC     fu.EsBaja
# MAGIC     --current_timestamp() as FechaCarga
# MAGIC
# MAGIC FROM operaciones.bronze_mte.fact_usuarios u
# MAGIC LEFT JOIN operaciones.gold_mte.dim_centrostrabajo ct
# MAGIC     on u.IdCentroTrabajo = ct.IdCentroTrabajo
# MAGIC LEFT JOIN operaciones.gold_mte.dim_empresas e
# MAGIC     on u.IdEmpresa = e.IdEmpresa
# MAGIC LEFT JOIN operaciones.gold_mte.dim_equipostrabajomultiarea etm
# MAGIC     on u.IdEquipoTrabajoMultiarea = etm.IdEquipoMultitarea
# MAGIC LEFT JOIN operaciones.gold_mte.dim_equipostrabajohbu ethbu
# MAGIC     on u.IdEquipoTrabajohBU = ethbu.IdEquipohBU
# MAGIC LEFT JOIN operaciones.gold_mte.dim_gruponivel gn
# MAGIC     on u.IdGrupoNivel = gn.IdGrupoNivel
# MAGIC LEFT JOIN operaciones.gold_mte.dim_nivelprofesional np
# MAGIC     on u.IdNivelProfesional = np.IdNivelProfesional
# MAGIC LEFT JOIN operaciones.gold_mte.dim_carreraprofesional cp
# MAGIC     on u.IdCarreraProfesional = cp.IdCarreraProfesional
# MAGIC LEFT JOIN operaciones.gold_mte.dim_subhbu shbu
# MAGIC     ON u.IdSubhBU = shbu.IdSubhBU
# MAGIC LEFT JOIN operaciones.gold_mte.dim_subhgr shgr
# MAGIC     ON u.IdSubhGR = shgr.IdSubhGR
# MAGIC LEFT JOIN operaciones.gold_mte.dim_hbu hbu
# MAGIC     ON u.IdhBU = hbu.IdhBU
# MAGIC LEFT JOIN operaciones.gold_mte.dim_hgr hgr
# MAGIC     ON u.IdhGR = hgr.IdhGR
# MAGIC LEFT JOIN operaciones.gold_mte.dim_hgz hgz
# MAGIC     ON u.IdhGZ = hgz.IdhGZ
# MAGIC LEFT JOIN operaciones.gold_mte.dim_hma hma
# MAGIC     ON u.IdhMA = hma.IdhMA
# MAGIC LEFT JOIN operaciones.gold_mte.fact_usuarios fu 
# MAGIC     ON cast(u.ID as INTEGER) = fu.IdUsuario
# MAGIC LEFT JOIN operaciones.bronze_mte.view_usuarios vu
# MAGIC     ON cast(u.ID as INTEGER) = cast(vu.ID as INTEGER)
# MAGIC LEFT JOIN people.gold_mteia.usuarios_ia uia
# MAGIC     ON u.ID = uia.ID
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN ResponsablesAgregadoshBU ra
# MAGIC     ON u.IdhBU = ra.IdhBU
# MAGIC LEFT JOIN ResponsablesAgregadoshMA rah
# MAGIC     ON u.IdhMA = rah.IdhMA
# MAGIC LEFT JOIN ResponsablesAgregadosEquipoMultiarea ram
# MAGIC     ON u.IdEquipoTrabajoMultiarea = ram.IdEquipoMultitarea
# MAGIC LEFT JOIN ResponsablesAgregadosSubhBU rasb
# MAGIC     ON u.IdSubhBU = rasb.IdSubhBU
# MAGIC LEFT JOIN ResponsablesAgregadoshGZ rahgz
# MAGIC     ON u.IdhGZ = rahgz.IdhGZ
# MAGIC LEFT JOIN ResponsablesAgregadoshGR rahgr
# MAGIC     ON u.IdhGR = rahgr.IdhGR
# MAGIC LEFT JOIN ResponsablesAgregadosSubhGR rasgr
# MAGIC     ON u.IdSubhGR = rasgr.IdSubhGR
# MAGIC LEFT JOIN ResponsablesAgregadosEquipoHBU raeh
# MAGIC     ON u.IdEquipoTrabajohBU = raeh.IdEquipohBU
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN aux_approver_id_map map
# MAGIC     ON uia.AprobadorVacaciones = map.AprobadorVacaciones
# MAGIC
# MAGIC
# MAGIC WHERE u.IdhBU = '14' -- DATA&IA

# COMMAND ----------

# DBTITLE 1,BBDD params
AzureDB_Hostname = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Objetivos-DB-hostname")
AzureDB_Database = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Objetivos-DB-database")
AzureDB_Datauser = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Objetivos-DB-username")
AzureDB_Datapassword = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "Objetivos-DB-password")

# COMMAND ----------

# DBTITLE 1,check autenticacion
# def test_azure_sql_connection(server, database, client_id, client_secret, timeout=180):
#     # Add debug information
#     print(f"\nPython version: {sys.version}")
#     print(f"PyODBC version: {pyodbc.version}")
    
#     # List available drivers
#     print("\nAvailable ODBC drivers:")
#     for driver in pyodbc.drivers():
#         print(f"  - {driver}")
#     """
#     Test connection to Azure SQL Server using a service principal.
#     Returns True if connection succeeds, False otherwise.
    
#     Parameters:
#     - server: The SQL Server hostname (should be in format: server.database.windows.net)
#     - database: The database name
#     - client_id: The service principal client ID
#     - client_secret: The service principal client secret
#     - timeout: Connection timeout in seconds
#     """
#     # First, let's check if we can reach the server
#     print(f"Checking if server {server} is reachable...")
#     try:
#         # Extract hostname without port if present
#         hostname = server.split(':')[0]
        
#         # If server doesn't end with windows.net, append the full domain
#         if ".database.windows.net" not in hostname:
#             full_hostname = f"{hostname}.database.windows.net"
#             print(f"Adding Azure SQL domain suffix: {full_hostname}")
#         else:
#             full_hostname = hostname
            
#         # Try to connect to SQL port (1433 by default)
#         port = 1433
#         if ':' in server:
#             port = int(server.split(':')[1])
            
#         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         sock.settimeout(5)  # 5 second timeout for the socket test
#         result = sock.connect_ex((full_hostname, port))
#         if result == 0:
#             print(f"Server {full_hostname}:{port} is reachable.")
#             server = full_hostname  # Use the full hostname for the connection
#         else:
#             print(f"Warning: Cannot reach server {full_hostname}:{port}. Connection might fail.")
#         sock.close()
#     except Exception as e:
#         print(f"Network check failed: {str(e)}")
    
#     try:
#         # Connection string for service principal authentication
#         conn_str = (
#             "Driver={ODBC Driver 17 for SQL Server};"
#             f"Server={server};"
#             f"Database={database};"
#             "Authentication=ActiveDirectoryServicePrincipal;"
#             f"UID={client_id};"
#             f"PWD={client_secret};"
#             "Encrypt=yes;"
#             "TrustServerCertificate=no;"
#             f"Connection Timeout={timeout};"
#         )
        
#         print(f"Attempting to connect to Azure SQL Server using service principal...")
#         print(f"Server: {server}, Database: {database}")
#         start_time = time.time()
#         conn = pyodbc.connect(conn_str)
#         elapsed_time = time.time() - start_time
#         print(f"Connection successful! (took {elapsed_time:.2f} seconds)")
        
#         # Test executing a simple query
#         cursor = conn.cursor()
#         cursor.execute("SELECT @@VERSION")
#         version = cursor.fetchone()[0]
#         print(f"SQL Server version: {version[:50]}...")
        
#         # Close connections
#         cursor.close()
#         conn.close()
#         print("Connection closed properly.")
        
#         return True
        
#     except Exception as e:
#         print(f"Connection failed: {str(e)}")
#         return False


# # Test the connection
# # Make sure these variables are correctly defined with your service principal details
# connection_works = test_azure_sql_connection(
#     server=AzureDB_Hostname,  # Make sure this includes the full domain: yourserver.database.windows.net
#     database=AzureDB_Database,
#     client_id=AzureDB_Datauser,  # This should be the service principal's client ID
#     client_secret=AzureDB_Datapassword,  # This should be the service principal's secret
#     timeout=180  # Extended timeout to 3 minutes
# )

# if connection_works:
#     print("\nYou can now proceed with your data import code.")
# else:
#     print("\nPlease resolve the connection issues before attempting data import.")

# # Extra helper functions for testing alternative authentication methods

# def test_azure_sql_connection_ad_password(server, database, username, password, timeout=180):
#     """Test connection using ActiveDirectoryPassword authentication"""
#     try:
#         conn_str = (
#             "Driver={ODBC Driver 17 for SQL Server};"
#             f"Server={server};"
#             f"Database={database};"
#             "Authentication=ActiveDirectoryPassword;"
#             f"UID={username};"
#             f"PWD={password};"
#             "Encrypt=yes;"
#             "TrustServerCertificate=no;"
#             f"Connection Timeout={timeout};"
#         )
        
#         print("Attempting to connect with ActiveDirectoryPassword...")
#         conn = pyodbc.connect(conn_str)
#         print("Connection successful!")
#         conn.close()
#         return True
#     except Exception as e:
#         print(f"ActiveDirectoryPassword connection failed: {str(e)}")
#         return False

# def test_azure_sql_connection_sql(server, database, username, password, timeout=180):
#     """Test connection using SQL Server authentication"""
#     try:
#         conn_str = (
#             "Driver={ODBC Driver 17 for SQL Server};"
#             f"Server={server};"
#             f"Database={database};"
#             f"UID={username};"
#             f"PWD={password};"
#             "Encrypt=yes;"
#             "TrustServerCertificate=no;"
#             f"Connection Timeout={timeout};"
#         )
        
#         print("Attempting to connect with SQL Server authentication...")
#         conn = pyodbc.connect(conn_str)
#         print("Connection successful!")
#         conn.close()
#         return True
#     except Exception as e:
#         print(f"SQL Server authentication failed: {str(e)}")
#         return False

# # Uncomment to try alternative authentication methods if needed

# # Try with ActiveDirectoryPassword instead (if it's actually a user account)
# if not connection_works:
#     print("\nTrying with ActiveDirectoryPassword authentication...")
#     connection_works = test_azure_sql_connection_ad_password(
#         server=AzureDB_Hostname,
#         database=AzureDB_Database,
#         username=AzureDB_Datauser,  # This would be username/email instead
#         password=AzureDB_Datapassword  # This would be password instead
#     )

# # Or try with SQL Server Authentication if available
# if not connection_works:
#     print("\nTrying with SQL Server authentication...")
#     connection_works = test_azure_sql_connection_sql(
#         server=AzureDB_Hostname,
#         database=AzureDB_Database,
#         username=AzureDB_Datauser,  # SQL Server username
#         password=AzureDB_Datapassword   # SQL Server password
#     )

# COMMAND ----------

# DBTITLE 1,insert table
def connect_to_azure_sql(server, database, username, password, timeout=180):
    """
    Connect to Azure SQL database using SQL Server authentication.
    
    Parameters:
    - server: Server hostname
    - database: Database name
    - username: SQL username
    - password: SQL password
    - timeout: Connection timeout in seconds
    
    Returns:
    - Connection object if successful, None if failed
    """
    try:
        # Connection string for SQL Server authentication
        conn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            f"Server={server};"
            f"Database={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            f"Connection Timeout={timeout};"
        )
        
        print(f"Connecting to {database} on {server}...")
        start_time = time.time()
        conn = pyodbc.connect(conn_str)
        elapsed_time = time.time() - start_time
        print(f"Connection successful! (took {elapsed_time:.2f} seconds)")
        return conn
        
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return None

def get_target_table_schema(conn, table_name, schema='STAGING'):
    """
    Retrieve the schema of the target table to ensure proper data type conversion.
    
    Parameters:
    - conn: pyodbc connection object
    - table_name: Name of the table
    - schema: Database schema name
    
    Returns:
    - Dictionary with column names as keys and SQL Server data types as values
    """
    try:
        cursor = conn.cursor()
        full_table_name = f"[{schema}].[{table_name}]"
        
        # Query to get column information
        query = f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH, 
            NUMERIC_PRECISION, 
            NUMERIC_SCALE
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_SCHEMA = '{schema}' AND 
            TABLE_NAME = '{table_name}'
        ORDER BY 
            ORDINAL_POSITION
        """
        
        cursor.execute(query)
        columns_info = {}
        
        for row in cursor.fetchall():
            col_name, data_type, char_max_len, num_precision, num_scale = row
            columns_info[col_name] = {
                'data_type': data_type,
                'char_max_len': char_max_len,
                'num_precision': num_precision,
                'num_scale': num_scale
            }
        
        cursor.close()
        return columns_info
        
    except Exception as e:
        print(f"Error getting table schema: {str(e)}")
        return {}

def clean_dataframe_for_sql(df, table_schema):
    """
    Clean and convert DataFrame values to match SQL Server table schema.
    
    Parameters:
    - df: pandas DataFrame
    - table_schema: Dictionary with column information from get_target_table_schema
    
    Returns:
    - Cleaned DataFrame with compatible types
    """
    cleaned_df = df.copy()
    
    # Loop through each column and apply appropriate conversions
    for col in cleaned_df.columns:
        if col in table_schema:
            sql_type = table_schema[col]['data_type'].lower()
            
            # Replace None with appropriate NULL value based on column type
            # Handle various data types
            if sql_type in ('int', 'bigint', 'smallint', 'tinyint'):
                # Convert to appropriate integer type or None
                cleaned_df[col] = cleaned_df[col].apply(
                    lambda x: int(x) if x is not None and pd.notna(x) and str(x).strip() != '' else None
                )
            
            elif sql_type in ('float', 'real', 'numeric', 'decimal'):
                # Convert to float or None, handling empty strings
                cleaned_df[col] = cleaned_df[col].apply(
                    lambda x: float(x) if x is not None and pd.notna(x) and str(x).strip() != '' else None
                )
            
            elif sql_type in ('date', 'datetime', 'datetime2', 'smalldatetime'):
                # Ensure dates are properly formatted or set to None
                cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
            
            elif sql_type in ('char', 'varchar', 'nvarchar', 'nchar', 'text', 'ntext'):
                # Convert to string, handle None values
                cleaned_df[col] = cleaned_df[col].apply(
                    lambda x: str(x) if x is not None and pd.notna(x) else None
                )
                
                # Truncate if necessary based on max length
                max_len = table_schema[col]['char_max_len']
                if max_len is not None and max_len > 0:
                    cleaned_df[col] = cleaned_df[col].apply(
                        lambda x: x[:max_len] if x is not None else None
                    )
    
    # Replace NaN values with None for better SQL compatibility
    cleaned_df = cleaned_df.replace({np.nan: None})
    
    return cleaned_df

def overwrite_table_with_dataframe(conn, df, table_name, schema='STAGING'):
    """
    Overwrites a table with data from a pandas DataFrame.
    
    Parameters:
    - conn: pyodbc connection object
    - df: pandas DataFrame containing the data
    - table_name: Name of the table to overwrite
    - schema: Database schema name
    
    Returns:
    - True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        full_table_name = f"[{schema}].[{table_name}]"
        
        # Get the target table schema
        table_schema = get_target_table_schema(conn, table_name, schema)
        
        if not table_schema:
            print(f"Error: Could not retrieve schema for table {full_table_name}")
            return False
        
        # Print the first few rows of the DataFrame for debugging
        print("\nFirst 3 rows of DataFrame before cleaning:")
        print(df.iloc[:3].to_string())
        
        # Clean and convert DataFrame data to match SQL table schema
        cleaned_df = clean_dataframe_for_sql(df, table_schema)
        
        # print("\nFirst 3 rows of cleaned DataFrame:")
        # print(cleaned_df.iloc[:3].to_string())
        
        # 1. Truncate the existing table
        print(f"Truncating table {full_table_name}...")
        cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        conn.commit()
        print("Table truncated successfully")
        
        # 2. Insert the data from DataFrame
        print(f"Inserting {len(cleaned_df)} rows into {full_table_name}...")
        start_time = time.time()
        
        # Prepare the insert statement based on DataFrame columns
        columns = ', '.join([f"[{col}]" for col in cleaned_df.columns])
        placeholders = ', '.join(['?' for _ in cleaned_df.columns])
        insert_query = f"INSERT INTO {full_table_name} ({columns}) VALUES ({placeholders})"
        
        # Execute batch insert
        row_count = 0
        batch_size = 100  # Smaller batch size for better error tracking
        error_rows = []
        
        for i in range(0, len(cleaned_df), batch_size):
            batch = cleaned_df.iloc[i:i+batch_size]
            batch_data = [tuple(row) for row in batch.values]
            
            try:
                cursor.executemany(insert_query, batch_data)
                conn.commit()
                row_count += len(batch)
                print(f"  Inserted {row_count} of {len(cleaned_df)} rows...")
            except Exception as batch_error:
                # If batch fails, try inserting rows one by one to identify problematic rows
                print(f"  Batch insert failed. Attempting row-by-row insert for this batch.")
                conn.rollback()
                
                for row_idx, row_data in enumerate(batch_data):
                    try:
                        cursor.execute(insert_query, row_data)
                        conn.commit()
                        row_count += 1
                    except Exception as row_error:
                        absolute_row_idx = i + row_idx
                        print(f"  Error at row {absolute_row_idx}: {str(row_error)}")
                        error_rows.append((absolute_row_idx, str(row_error)))
                        conn.rollback()
        
        elapsed_time = time.time() - start_time
        
        if error_rows:
            print(f"\nEncountered {len(error_rows)} problematic rows:")
            for row_idx, error_msg in error_rows[:5]:
                print(f"  Row {row_idx}: {error_msg}")
            if len(error_rows) > 5:
                print(f"  ... and {len(error_rows) - 5} more errors")
        
        print(f"Data insertion complete! Successfully inserted {row_count} of {len(cleaned_df)} rows (took {elapsed_time:.2f} seconds)")
        cursor.close()
        return row_count > 0
        
    except Exception as e:
        print(f"Error overwriting table: {str(e)}")
        # Attempt to rollback if possible
        try:
            conn.rollback()
        except:
            pass
        return False

def main():
    """
    Main function to handle database connection and data insertion.
    """
    # Connection parameters
    server = AzureDB_Hostname         
    database = AzureDB_Database       
    username = AzureDB_Datauser   
    password = AzureDB_Datapassword    
    table_name = "EMPLOYEE_MTE"        
    
    # Connect to the database
    conn = connect_to_azure_sql(server, database, username, password)
    
    if conn:
        try:
            # Get the DataFrame from Spark
            print("Fetching data from Spark SQL...")
            df = spark.sql("SELECT * FROM tabla_matias")
            df = df.toPandas()
            
            print(f"\nPrepared DataFrame with {len(df)} rows and {len(df.columns)} columns")
            
            # Display data types of the DataFrame columns
            print("\nDataFrame column data types:")
            for col, dtype in df.dtypes.items():
                print(f"  {col}: {dtype}")
            
            # Overwrite the table with the DataFrame
            success = overwrite_table_with_dataframe(conn, df, table_name)
            
            if success:
                print("\nData update completed successfully!")
            else:
                print("\nData update failed or partially completed.")
            
            # Close the connection
            # conn.close()
            # print("Connection closed.")
            
        except Exception as e:
            print(f"Error in main process: {str(e)}")
            import traceback
            print(traceback.format_exc())
    else:
        print("Cannot proceed without a database connection.")

if __name__ == "__main__":
    main()

# COMMAND ----------

# DBTITLE 1,refresh procedures
def execute_stored_procedure(conn, procedure_name):
    """
    Execute a stored procedure and print the results.
    
    Parameters:
    - conn: pyodbc connection object
    - procedure_name: Full name of the stored procedure (schema.name)
    
    Returns:
    - True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        print(f"Executing stored procedure {procedure_name}...")
        
        start_time = time.time()
        cursor.execute(f"EXEC {procedure_name}")
        
        # Fetch and display any result sets
        try:
            results = cursor.fetchall()
            if results:
                print(f"Procedure returned {len(results)} rows:")
                for i, row in enumerate(results[:5]):  # Show first 5 rows
                    print(f"  Row {i+1}: {row}")
                if len(results) > 5:
                    print(f"  ... and {len(results) - 5} more rows")
        except:
            # No result set or already consumed
            pass
            
        # Commit the transaction
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"Stored procedure {procedure_name} executed successfully! (took {elapsed_time:.2f} seconds)")
        cursor.close()
        return True
        
    except Exception as e:
        print(f"Error executing stored procedure {procedure_name}: {str(e)}")
        try:
            conn.rollback()
        except:
            pass
        return False

def refresh_mte_data():
    """
    Execute the MTE data refresh stored procedures in sequence.
    """
    # Connection parameters (using the same as in the main script)
    server = AzureDB_Hostname
    database = AzureDB_Database
    username = AzureDB_Datauser
    password = AzureDB_Datapassword
    
    # Connect to the database
    conn = connect_to_azure_sql(server, database, username, password)
    
    if conn:
        try:
            # Execute the first stored procedure
            first_proc = "[REFINED].[usp_Refresh_MTE_DATA]"
            success1 = execute_stored_procedure(conn, first_proc)
            
            # If the first procedure was successful, execute the second one
            if success1:
                print("\nFirst procedure completed successfully. Proceeding to execute the next procedure...")
                second_proc = "[REFINED].[usp_Refresh_MTE_HIERARCHIES]"
                success2 = execute_stored_procedure(conn, second_proc)
                
                if success2:
                    print("\nBoth stored procedures executed successfully!")
                else:
                    print("\nSecond stored procedure failed or returned an error.")
            else:
                print("\nFirst stored procedure failed. Skipping the second procedure.")
            
            # Close the connection
            conn.close()
            print("Database connection closed.")
            
        except Exception as e:
            print(f"Error in stored procedure execution process: {str(e)}")
            import traceback
            print(traceback.format_exc())
            
            # Try to close the connection if it exists
            try:
                conn.close()
                print("Database connection closed.")
            except:
                pass
    else:
        print("Cannot proceed without a database connection.")

# Execute the function
refresh_mte_data()