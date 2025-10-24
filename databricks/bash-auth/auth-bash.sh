export CLIENT_ID=b6690399-b716-4941-9688-297d9527c223
export CLIENT_SECRET=dose4cb456b2c33853e77e6fc63997dd3310

reposnse = $(curl --request POST \
--url https://accounts.cloud.databricks.com/oidc/accounts/4d3432d4-b29c-4e3a-bde4-9a5819ee2d85/v1/token \
--user "$CLIENT_ID:$CLIENT_SECRET" \
--data 'grant_type=client_credentials&scope=all-apis')


# Extraer el token de acceso del JSON de respuesta
OAUTH_TOKEN=$(echo $response | jq -r .access_token)

# Comprobar si el token fue extraído correctamente
if [ -z "$OAUTH_TOKEN" ]; then
  echo "Error al obtener el token de acceso"
  exit 1
fi

# Usar el token de acceso para realizar otra petición
# Asegúrate de reemplazar <workspace-URL> con la URL de tu espacio de trabajo
curl --request GET \
  --header "Authorization: Bearer $OAUTH_TOKEN" \
  'https://<workspace-URL>/api/2.0/clusters/list'