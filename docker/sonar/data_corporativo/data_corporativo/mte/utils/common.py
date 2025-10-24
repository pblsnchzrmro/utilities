# Databricks notebook source
import requests
import json

# COMMAND ----------

#Función para extraer el token de la API 'MTE-API-AI'
def API_MTE_token(): 
        # Conseguir el token en la API: MTE-API-Data
        URL = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-url")
        client_id = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-client-id")
        client_secret = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-client-secret")
        scope = dbutils.secrets.get(scope = "KeyVaultDataMTC", key = "API-MTE2-token-client-scope")

        myobj = {'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': scope}

        r = requests.post(url = URL,data = myobj)
        data = r.json()
        token = data['access_token']
        header = {'Authorization': 'Bearer '+ token}
        return header
    

# COMMAND ----------

def request_data(url, token):
    try:
        response = requests.get(url, headers=token)
        if response.status_code == 200:
            # print("Respuesta exitosa:")
            # print(response.json()) # Imprimir respuesta en formato JSON
            return response.json()
        else:
            # print(f"Error: {response.status_code}")
            # print(response.text)  # Imprimir el texto de la respuesta de error
            return(f"Error: {response.status_code} " + response.text)
    except requests.exceptions.RequestException as e:
        return (f"Error en la solicitud: {e}")