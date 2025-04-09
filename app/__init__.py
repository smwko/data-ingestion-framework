import os
from flask import Flask, current_app
from config import SECRET_KEY  # Importa las variables globales definidas en config.py
from pyspark.sql import SparkSession
from src.utils.logger import get_logger, WebLogHandler

def create_app():
    # Configurar la aplicación Flask y especificar la carpeta de templates si no es la por defecto
    app = Flask(__name__, template_folder='templates')
    app.secret_key = SECRET_KEY

    # Inicializar la lista de logs para mostrarlos en la web
    app.logs = []

    # Inicializar la sesión Spark y asociarla a la app
    app.spark = SparkSession.builder.appName("DataIngestionDynamic").getOrCreate()

    # Configurar y registrar el logger (incluye un handler para enviar logs a la web)
    logger = get_logger()
    web_handler = WebLogHandler(app)
    logger.addHandler(web_handler)
    logger.info("Sesión Spark iniciada")

    # Registrar Blueprints
    from app.routes.main_routes import main_bp
    app.register_blueprint(main_bp)

  

    return app
