# Databricks notebook source
import requests
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Función para primer y ultimo día del mes

# COMMAND ----------

# Utilizar para el mes actual:
prev_date = datetime.today() - relativedelta(months=1)
month, year = prev_date.month, prev_date.year

# COMMAND ----------

def first_last_month(month: int, year: int):
    first_day = datetime(year, month, 1)
    
    # El último día se obtiene sumando un mes al primer día y restando un día
    if month == 12:
        last_day = datetime(year, 12, 31)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(days=1)
    
    return first_day.strftime("%m/%d/%Y"), last_day.strftime("%m/%d/%Y")

# COMMAND ----------

first_day, last_day = first_last_month(month, year)
print(first_day, last_day)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Token de autenticación

# COMMAND ----------

url = "https://app.nailted.com/api/authentication"
body = {
    "strategy": "user",
    "token": "LJKQxZJgXCl9nMUQacAtlkVzGZ1CV6ul"  # Llevar a secret manager
}

# COMMAND ----------

def get_token_nailted():
    try:
        response = requests.post(url, json=body)

        # Comprobar el estado de la respuesta
        if response.status_code == 200 or response.status_code == 201:
            print("Respuesta exitosa:")
            print(f"{response.status_code}")
            # print(response.json())  # Imprimir respuesta en formato JSON
            return response.json()['accessToken']
        else:
            print(f"Error: {response.status_code}")
            print(response.text)  # Imprimir el texto de la respuesta de error

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")

# COMMAND ----------

def request_data(url_metrics, headers, parameters):
    try:
        response = requests.get(url_metrics, headers=headers, params=parameters)

        # Comprobar el estado de la respuesta
        if response.status_code == 200 or response.status_code == 201:
            print("Respuesta exitosa:")
            print(f"{response.status_code}")
            # print(response.json())  # Imprimir respuesta en formato JSON
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(response.text)  # Imprimir el texto de la respuesta de error

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")