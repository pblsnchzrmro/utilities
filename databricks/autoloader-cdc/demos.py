# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.list_demos()


# COMMAND ----------

dbdemos.install('auto-loader')

# COMMAND ----------

dbdemos.install('cdc-pipeline')
