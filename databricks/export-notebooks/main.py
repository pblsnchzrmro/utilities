# Prueba que se hizo (sin terminar) para reconocer cuales son los notebook que están impiedendo la descarga del proyecto en .dbc
import requests
import os


# Configuración
BASE_URL = "https://adb-2620936039785376.16.azuredatabricks.net"
TOKEN = ""
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}
SIZE_LIMIT = 10485760  # 10,485,760 bytes


def list_notebooks(path="/Proyecto SIT/3-Load"):
    """Devuelve una lista de paths de notebooks en el directorio especificado."""
    resp = requests.get(f"{BASE_URL}/api/2.0/workspace/list", headers=HEADERS, params={"path": path})
    resp.raise_for_status()
    
    items = resp.json().get("objects", [])
    notebooks = [item['path'] for item in items if item['object_type'] == 'NOTEBOOK']
    
    # Recursivamente obtiene notebooks en directorios
    for item in items:
        if item['object_type'] == 'DIRECTORY':
            notebooks.extend(list_notebooks(item['path']))
    
    return notebooks

def get_notebook_size(notebook_path):
    """Devuelve el tamaño de un notebook en bytes sin descargarlo."""
    resp = requests.get(f"{BASE_URL}/api/2.0/workspace/export", headers=HEADERS, params={"path": notebook_path, "format": "DBC"})
    resp.raise_for_status()
    
    # Usar content para obtener el contenido binario del notebook
    notebook_data = resp.content
    return len(notebook_data)

def main():
    notebook_paths = list_notebooks()
    exceeding_notebooks = []
    
    for path in notebook_paths:
        size = get_notebook_size(path)
        if size > SIZE_LIMIT:
            exceeding_notebooks.append((path, size))
    
    # Guardar en un archivo .txt
    with open('notebooks.txt', 'w') as f:
        for path, size in exceeding_notebooks:
            f.write(f"{path} {size} bytes\n")

if __name__ == "__main__":
    main()







