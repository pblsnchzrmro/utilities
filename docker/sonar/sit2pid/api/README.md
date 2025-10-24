# DataEstur API

Api de prueba para migración de dataestur a la nueva arquitectura sit

## Requisitos previos

Antes de comenzar, asegúrate de tener instalado lo siguiente:

- [Docker](https://www.docker.com/)

## Construcción de la imagen Docker

Para construir la imagen Docker, ejecuta el siguiente comando en el directorio raíz del proyecto:

```bash
docker build -t dataestur-api .
```

## Construcción de la imagen Docker

Para levantar el contenedor, ejecuta el siguiente comando en el directorio raíz del proyecto:

```bash
docker run -p 5000:5000 --name dataestur-api-container dataestur-api
```
