import azure.functions as func
import logging
import os
import requests

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="", methods=["GET"])
def home(req: func.HttpRequest) -> func.HttpResponse:
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        return func.HttpResponse(html, mimetype="text/html")
    except Exception as e:
        return func.HttpResponse(f"Error cargando index.html: {str(e)}", status_code=500)


@app.route(route="exec_databricks_job", methods=["POST"])
def exec_databricks_job(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure Function: Ejecutando job en Databricks.')

    DATABRICKS_INSTANCE = "https://adb-3842051123482980.0.azuredatabricks.net"
    TOKEN = ""
    JOB_ID = 1018813121271181

    payload = {
        "job_id": JOB_ID
    }

    try:
        response = requests.post(
            f"{DATABRICKS_INSTANCE}/api/2.2/jobs/run-now",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json=payload
        )

        if response.status_code == 200:
            run_id = response.json().get("run_id")
            logging.info(f"Job ejecutado correctamente. Run ID: {run_id}")
            return func.HttpResponse(f"Job ejecutado correctamente. Run ID: {run_id}", status_code=200)
        else:
            logging.error(f"Error al ejecutar el job: {response.status_code} - {response.text}")
            return func.HttpResponse(
                f"Error al ejecutar el job: {response.text}",
                status_code=response.status_code
            )

    except Exception as e:
        logging.exception("Error inesperado al ejecutar el job.")
        return func.HttpResponse(f"Error inesperado: {str(e)}", status_code=500)