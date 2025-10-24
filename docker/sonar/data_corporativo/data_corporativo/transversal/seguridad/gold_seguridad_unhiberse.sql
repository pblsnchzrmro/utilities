-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS transversal.gold_seguridad

-- COMMAND ----------

-- DBTITLE 1,proyecto unhiberse
CREATE OR REPLACE TABLE transversal.gold_seguridad.seguridad_unhiberse AS
SELECT DISTINCT
    IdProyecto,
    TRIM(email) AS Permiso
FROM (
    SELECT 
        IdProyecto,
        EXPLODE(SPLIT(email_list.emails, ',')) AS email
    FROM (
        SELECT 
            project.code AS IdProyecto,
            ARRAY_JOIN(
                ARRAY(
                    hgz.ResponsableEmail,
                    hgr.ResponsableEmail,
                    subhgr.ResponsableEmail,
                    hma.ResponsableEmail,
                    empresas.ResponsableEmail,
                    hbu.ResponsableEmail,
                    subhbu.ResponsableEmail,
                    equipostrabajo.ResponsableEmail
                ), ','
            ) AS emails
        FROM
            operaciones.silver_unhiberse.project AS project
        LEFT JOIN operaciones.bronze_mte.empresas AS empresas
            ON empresas.ID = project.company_code
        LEFT JOIN operaciones.bronze_mte.hma AS hma
            ON hma.ID = project.hma_code
        LEFT JOIN operaciones.bronze_mte.hbu AS hbu
            ON hbu.ID = project.hbu_code
        LEFT JOIN operaciones.bronze_mte.subhbu AS subhbu
            ON project.sub_hbu_code = subhbu.ID
        LEFT JOIN operaciones.bronze_mte.equipostrabajohbu AS equipostrabajo
            ON equipostrabajo.ID = project.work_team_code
        LEFT JOIN operaciones.bronze_mte.hgr AS hgr
            ON hgr.ID = project.region_code
        LEFT JOIN operaciones.bronze_mte.subhgr AS subhgr
            ON subhgr.ID = project.sub_region_code
        LEFT JOIN operaciones.bronze_mte.hgz AS hgz
            ON hgz.ID = project.zone_code
    ) AS email_list
) AS exploded_emails
WHERE email IS NOT NULL AND email <> '';