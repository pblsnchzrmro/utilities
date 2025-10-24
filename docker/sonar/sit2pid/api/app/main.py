from routes import create_app
from common.setup_logging import setup_logger  # Importa el logger personalizado

# Configurar el logger para el punto de entrada de la aplicación
logger = setup_logger()

app = create_app()

if __name__ == '__main__':
    logger.info("Starting the Flask application")
    app.run(debug=True, host='0.0.0.0', port=5000)
