# Ejecución de Gitleaks rápida

```powershell
docker run --rm -v "${PWD}:/path" zricethezav/gitleaks:latest detect --source=/path
docker run --rm -v "${PWD}:/usr/src" sonarsource/sonar-scanner-cli

