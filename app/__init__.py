from flask import Flask, current_app
from config import SECRET_KEY  # Importa las variables globales definidas en config.py
from pyspark.sql import SparkSession
from src.utils.logger import get_logger, WebLogHandler
import logging


def create_app():
    # Crea la instancia de la aplicación Flask y define la carpeta de plantillas.
    app = Flask(__name__, template_folder='templates')
    
    # Establece la clave secreta para la aplicación (utilizada para la encriptación de sesiones, cookies, etc.)
    app.secret_key = SECRET_KEY

    # Inicializa una lista vacía para almacenar logs, que luego se mostrarán en la interfaz web.
    app.logs = []

    # Crea una sesión Spark global que se usará en toda la aplicación.
    app.spark = SparkSession.builder.appName("DataIngestionDynamic").getOrCreate()

    # Configura el logger de la aplicación:
    # 1. Se obtiene una instancia del logger usando get_logger() (definido en src/utils/logger.py).
    logger = get_logger()
    
    # 2. Crea una instancia de WebLogHandler que tomará la app para almacenar los logs en app.logs.
    web_handler = WebLogHandler(app)
    
    # 3. Define el formato de los mensajes de log.
    web_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s'))
    
    # 4. Agrega el WebLogHandler al logger para que los mensajes se almacenen en la lista de logs.
    logger.addHandler(web_handler)

    # Registra un mensaje informativo indicando que la sesión Spark se ha iniciado correctamente.
    logger.info("Sesión Spark iniciada")

    # Importa y registra el blueprint con las rutas de la aplicación.
    # Esto agrupa todas las rutas (como index, upload, etc.) en el blueprint main_bp.
    from app.routes.main_routes import main_bp
    app.register_blueprint(main_bp)

    # Configura el teardown (finalización del contexto de la aplicación).
    # En este caso se registra un mensaje de log sin detener la sesión Spark, 
    # para mantenerla activa entre peticiones.
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        current_app.logger.info("Teardown: La sesión Spark se mantiene activa.")

    # Retorna la instancia configurada de la aplicación.
    return app

