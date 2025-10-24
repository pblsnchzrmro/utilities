# Databricks notebook source
# DBTITLE 1,usuario mte
CREATE OR REPLACE TABLE transversal.gold_seguridad.seguridad_mte AS
WITH ResponsibleEmails AS (
    SELECT 
        cast(u.ID as INTEGER) as IdUsuario,
        EXPLODE(
            SPLIT(
                ARRAY_JOIN(
                    ARRAY(
                        hma.ResponsableEmail,
                        hbu.ResponsableEmail,
                        shbu.ResponsableEmail,
                        ethbu.ResponsableEmail
                    ), ','
                ), ','
            )
        ) AS Permiso
    FROM operaciones.bronze_mte.fact_usuarios u
    LEFT JOIN operaciones.gold_mte.dim_hma hma
        ON u.IdhMA = hma.IdhMA
    LEFT JOIN operaciones.gold_mte.dim_hbu hbu
        ON u.IdhBU = hbu.IdhBU
    LEFT JOIN operaciones.gold_mte.dim_subhbu shbu
        ON u.IdSubhBU = shbu.IdSubhBU
    LEFT JOIN operaciones.gold_mte.dim_equipostrabajohbu ethbu
        ON u.IdEquipoTrabajohBU = ethbu.IdEquipohBU
)
SELECT DISTINCT
    IdUsuario,
    TRIM(Permiso) AS Permiso
FROM ResponsibleEmails
WHERE Permiso IS NOT NULL AND TRIM(Permiso) != '';