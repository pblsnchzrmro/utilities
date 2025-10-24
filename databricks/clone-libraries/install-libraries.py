# Script para copiar exactamente las librerías de un cluster a otro cluster, en su momento se utilizó para instalar las de segittur en extremadura
import requests
import json
import os

# Tus variables
databricks_instance_segittur = "https://adb-2620936039785376.16.azuredatabricks.net"
original_cluster_id = "1128-080412-59dv4101"
databricks_instance_extremadura = "https://adb-2620936039785376.16.azuredatabricks.net"
new_cluster_id = "0108-143107-qgab5he4"
token_segittur = "
token_extremadura = 

# Header para las solicitudes
headers_segittur = {
    "Authorization": f"Bearer {token_segittur}",
    "Content-Type": "application/json"
}

headers_extremadura = {
    "Authorization": f"Bearer {token_extremadura}",
    "Content-Type": "application/json"
}

# Obtenemos la lista de bibliotecas del clúster original
response = requests.get(
    f"{databricks_instance_segittur}/api/2.0/libraries/cluster-status?cluster_id={original_cluster_id}",
    headers=headers_segittur
)

if response.status_code != 200:
    print(f"No se ha podido obtener la lista de bibliotecas. Código de estado: {response.status_code}")
    exit()

libraries = response.json()['library_statuses']

# Convertir 'libraries' a una cadena JSON
libraries_json = json.dumps(libraries, indent=4)  # 'indent=4' es para una mejor legibilidad

# Obtén el directorio de trabajo actual
current_directory = os.getcwd()

# Construye la ruta completa del archivo
file_path = os.path.join(current_directory, 'libraries.json')

# Escribir esta cadena en un archivo
with open(file_path, 'w') as file:
    file.write(libraries_json)
    
# Iteramos sobre las bibliotecas e instalamos cada una en el nuevo clúster
for library in libraries:
    try:
        package = library["library"]["pypi"]["package"]
        print(package)
        data = {
            "cluster_id": new_cluster_id,
            "libraries": [
                {
                    "pypi": {
                        "package": package
                    }
                }
            ]
        }
        print(json.dumps(data))
        response = requests.post(
            f"{databricks_instance_extremadura}/api/2.0/libraries/install",
            headers=headers_extremadura,
            json = data
        )

        if response.status_code == 200:
            print(f"La biblioteca {package} se ha instalado con éxito.")
        else:
            print(f"No se ha podido instalar la biblioteca {package}. Código de estado: {response.status_code}")
    except:
        pass

