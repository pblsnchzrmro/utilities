-- Databricks notebook source
-- DBTITLE 1,Operaciones
GRANT USE CATALOG ON CATALOG operaciones TO data_managers;
GRANT SELECT ON SCHEMA operaciones.gold_kvp TO data_managers;
GRANT USE SCHEMA ON SCHEMA operaciones.gold_kvp TO data_managers;
GRANT SELECT ON SCHEMA operaciones.gold_unhiberse TO data_managers;
GRANT USE SCHEMA ON SCHEMA operaciones.gold_unhiberse TO data_managers;