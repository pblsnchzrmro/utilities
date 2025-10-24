# Databricks notebook source
# MAGIC %pip install xgboost

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow.sklearn

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Lectura de los datos

# COMMAND ----------

# dbutils.widgets.removeAll()
# dbutils.widgets.text('file', '')
# dbutils.widgets.text('model_name', '')
file = dbutils.widgets.get('file')
model_name = dbutils.widgets.get('model_name')


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronze/mlflow_charla

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Análisis de los datos

# COMMAND ----------

df = pd.read_csv(file)
display(df)

# COMMAND ----------

# Correlación
corr_matrix = df.corr()['price']

# Crear una tabla con los resultados
results_table = pd.DataFrame({'Variable': corr_matrix.index, 'Correlación con Price': corr_matrix.values})
display(results_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Variables de entrenamiento y de testeo

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df["price"], random_state=42)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Localización de experimentos y Random forest sencillo

# COMMAND ----------

mlflow.set_experiment("/Users/psanchezr@hiberus.com/airbnb_charla")

with mlflow.start_run(run_name="Basic RF Run") as run:
    # Creacion de modelo, entrenamiento, prediccion
    rf = RandomForestRegressor(random_state=42)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, model_name)

    # Log metrics
    mse = mean_squared_error(y_test, predictions)
    mlflow.log_metric("mse", mse)

    run_id = run.info.run_id
    experiment_id = run.info.experiment_id

    print(
        f"Inside MLflow Run with run_id `{run_id}` and experiment_id `{experiment_id}`"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Random Forest algo más complejo con métricas, parámetros y artifacts

# COMMAND ----------

def log_rf(model_name, experiment_id, run_name, params, X_train, X_test, y_train, y_test):
  
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
        # Creacion de modelo, entrenamiento, prediccion
        rf = RandomForestRegressor(**params)
        rf.fit(X_train, y_train)
        predictions = rf.predict(X_test)

        # Log model
        mlflow.sklearn.log_model(rf, model_name)

        # Log params
        mlflow.log_params(params)

        # Log metrics
        mlflow.log_metrics({
            "mse": mean_squared_error(y_test, predictions), 
            "mae": mean_absolute_error(y_test, predictions), 
            "r2": r2_score(y_test, predictions)
        })

        # Log feature importance
        importance = (pd.DataFrame(list(zip(df.columns, rf.feature_importances_)), columns=["Feature", "Importance"])
                      .sort_values("Importance", ascending=False))

        # Log plot
        fig, ax = plt.subplots()
        importance.plot.bar(ax=ax)
        plt.title("Feature Importances")
        mlflow.log_figure(fig, "feature_importances.png")

        return run.info.run_id

# COMMAND ----------

params = {
    "n_estimators": 100,
    "max_depth": 5,
    "random_state": 42
}

log_rf(model_name, experiment_id, "RF Complex", params, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Gradien Boosting

# COMMAND ----------

def log_gb(model_name, experiment_id, run_name, params, X_train, X_test, y_train, y_test):
  
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
        # Creacion de modelo, entrenamiento, prediccion
        gb = GradientBoostingRegressor(**params)
        gb.fit(X_train, y_train)
        predictions = gb.predict(X_test)
        
        # Log model
        mlflow.sklearn.log_model(gb, model_name)
        
        # Log params
        mlflow.log_params(params)

        # Log metrics
        mlflow.log_metrics({
            "mse": mean_squared_error(y_test, predictions), 
            "mae": mean_absolute_error(y_test, predictions), 
            "r2": r2_score(y_test, predictions)
        })
      

        return run.info.run_id

# COMMAND ----------

params = {
    'n_estimators': 300,
    'max_depth': 5,
    'learning_rate': 0.1,
    'subsample': 0.8,
    'min_samples_split': 2
}

# COMMAND ----------

log_gb(model_name, experiment_id, "GB", params, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Creación del modelo de forma manual

# COMMAND ----------

import mlflow.pyfunc

model_version = "1"

model_version_uri = f"models:/{model_name}/{model_version}"


print(f"Loading registered model version from URI: '{model_version_uri}'")
model_version_1 = mlflow.pyfunc.load_model(model_version_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Predicción con la version 1 del modelo

# COMMAND ----------

model_version_1.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Añadimos varios experimentos

# COMMAND ----------

def log_gb(model_name, experiment_id, run_name, params, X_train, X_test, y_train, y_test):
  
    # Start the run manually
    run = mlflow.start_run(experiment_id=experiment_id, run_name=run_name)

    # Creacion de modelo, entrenamiento, prediccion
    gb = GradientBoostingRegressor(**params)
    gb.fit(X_train, y_train)
    predictions = gb.predict(X_test)
        
    # Log model 
    mlflow.sklearn.log_model(gb, model_name)

    # Log params 
    for key, value in params.items():
        mlflow.log_param(key, value)

    # Log metrics 
    mlflow.log_metric("mse", mean_squared_error(y_test, predictions))
    mlflow.log_metric("mae", mean_absolute_error(y_test, predictions))
    mlflow.log_metric("r2", r2_score(y_test, predictions))

    # End the run 
    mlflow.end_run()

    return run.info.run_id

# COMMAND ----------

from itertools import product

params = {
    'n_estimators': [50,100],
    'max_depth': [3,6],
    'learning_rate': [0.1],
    'subsample': [0.8],
    'min_samples_split': [2]
}

# Obtiene todas las combinaciones posibles
combinations = list(product(*params.values()))

# Crea un diccionario individual para cada combinación
param_dicts = [dict(zip(params.keys(), comb)) for comb in combinations]

for value in param_dicts:
  n_estimators = value["n_estimators"]
  max_depth = value["max_depth"]
  log_gb(model_name, experiment_id, f"GB_{n_estimators}_{max_depth}", value, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Manejo de modelos con MLFlow Client

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()

# COMMAND ----------

client.search_experiments()

# COMMAND ----------

#Obtencion de la última ejecuicion
experiment_id = 1800886221343360
runs = client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)
print(runs[0].data.metrics)

# COMMAND ----------

#Obtencion de la ejecucion con menor MAE
runs = client.search_runs(experiment_id, order_by=["metrics.mae desc"], max_results=1)
run_id = runs[0].info.run_id
print(run_id)

# COMMAND ----------

#Registro del nuevo modelo
model_uri = f"runs:/{run_id}/{model_name}"
model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

#Version en produccion
def obtain_version(mv):
  """
  Obtener version del modelo
  """
  for m in mv:
      return m.version
    
actual_version_production = obtain_version(client.get_latest_versions(model_name, stages=["Production"]))
print(actual_version_production)

# COMMAND ----------

# Última versión
last_version = obtain_version(client.get_latest_versions(model_name, stages=["Production","None"]))
print(last_version)

# COMMAND ----------

# Comparación versiones, si la nueva version es mayor que pase la anterior a archivado y la nuev a produccion
if int(actual_version_production) < int(last_version):
    client.transition_model_version_stage(model_name, actual_version_production, stage = "Archived")
    client.transition_model_version_stage(model_name, last_version, stage = "Production")

# COMMAND ----------

#Nueva version en produccion
actual_version_production = obtain_version(client.get_latest_versions(model_name, stages=["Production"]))
print(actual_version_production)

# COMMAND ----------

#Llamamos al modelo que esta en produccion
model_version_uri = f"models:/{model_name}/{actual_version_production}"

print(f"Loading registered model version from URI: '{model_version_uri}'")
model_prodcution_version= mlflow.pyfunc.load_model(model_version_uri)

# COMMAND ----------

# Prediccion con el nuevo modelo en produccion
model_prodcution_version.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Posibilidad de registrar el modelo directamente

# COMMAND ----------

def log_gb_model_register(model_name, experiment_id, run_name, params, X_train, X_test, y_train, y_test):
  
    # Start the run manually
    run = mlflow.start_run(experiment_id=experiment_id, run_name=run_name)

    # Creacion de modelo, entrenamiento, prediccion
    gb = GradientBoostingRegressor(**params)
    gb.fit(X_train, y_train)
    predictions = gb.predict(X_test)
        
    # Log model 
    mlflow.sklearn.log_model(gb, model_name, registered_model_name = model_name)

    # Log params 
    for key, value in params.items():
        mlflow.log_param(key, value)

    # Log metrics 
    mlflow.log_metric("mse", mean_squared_error(y_test, predictions))
    mlflow.log_metric("mae", mean_absolute_error(y_test, predictions))
    mlflow.log_metric("r2", r2_score(y_test, predictions))

    # End the run 
    mlflow.end_run()

    return run.info.run_id

# COMMAND ----------

params = {
    'n_estimators': 500,
    'max_depth': 2,
    'learning_rate': 0.1,
    'subsample': 0.8,
    'min_samples_split': 2
}

# COMMAND ----------

log_gb_model_register(model_name, experiment_id, "GB_model", params, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Autolog

# COMMAND ----------

# Enable auto-logging to MLflow to capture TensorBoard metrics.
def log_gb_autolog(experiment_id, run_name, params, X_train, X_test, y_train, y_test):
  
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
        mlflow.sklearn.autolog()
        # Creacion de modelo, entrenamiento, prediccion
        gb = GradientBoostingRegressor(**params)
        gb.fit(X_train, y_train)
        predictions = gb.predict(X_test)
        
        
        mlflow.end_run()

        return run.info.run_id

# COMMAND ----------

params = {
    'n_estimators': 100,
    'max_depth': 5,
    'learning_rate': 0.1,
    'subsample': 0.8,
    'min_samples_split': 2
}

# COMMAND ----------

log_gb_autolog(experiment_id, "GB_autolog", params, X_train, X_test, y_train, y_test)