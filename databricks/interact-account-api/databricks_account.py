import requests

# Configuración del service principal y Databricks
tenant_id = "9ce6cba4-87d9-4d32-b3e4-69118ed700c0"
client_id = "8"
client_secret = ""
databricks_instance = "https://adb-2665630080007048.8.azuredatabricks.net"

# 1. Solicita el token OAuth2 desde Azure AD
oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
oauth_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"  # Scope para Databricks
}

oauth_response = requests.post(oauth_url, data=oauth_data)
if oauth_response.status_code == 200:
    access_token = oauth_response.json().get("access_token")
    print("Token OAuth2 obtenido correctamente.")
else:
    print(f"Error en la obtención del token OAuth2: {oauth_response.status_code}")
    print(oauth_response.json())
    exit()


print(access_token)
# 2. Solicita la lista de usuarios en Databricks usando el token OAuth2
endpoint = f"{databricks_instance}/api/2.0/preview/scim/v2/Users"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

response = requests.get(endpoint, headers=headers)

if response.status_code == 200:
    users = response.json()
    print("Usuarios en la cuenta:")
    for user in users['Resources']:
        print(f"- {user['userName']}")
else:
    print(f"Error al obtener la lista de usuarios: {response.status_code}")
    print(response.text)