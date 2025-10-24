import requests
from databricks import sql

client_id = "8761f320-5154-44e0-87ba-0a02b31abc9d"
client_secret = "dosec017fc2615f72be307ed7df40d725f03"
workspace = "dbc-29208aa5-0c49.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/c957ddd0f7fabb5c"

token_url = f"https://{workspace}/oidc/v1/token"
resp = requests.post(
    token_url,
    auth=(client_id, client_secret),
    data={'grant_type': 'client_credentials', 'scope': 'all-apis'}
)
resp.raise_for_status()
access_token = resp.json()['access_token']

conn = sql.connect(
    server_hostname=workspace,
    http_path=http_path,
    access_token=access_token
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM range(10)")
print(cursor.fetchall())
cursor.close()
conn.close()