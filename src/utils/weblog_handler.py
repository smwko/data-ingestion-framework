from flask import Flask
from config import SECRET_KEY
from pyspark.sql import SparkSession
from src.utils.logger import get_logger, WebLogHandler  # Asegúrate de exportar WebLogHandler en utils/logger.py

def create_app():
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    # Inicia una lista de logs en la aplicación
    app.logs = []

    # Crear la sesión Spark y adjuntarla a la app
    app.spark = SparkSession.builder.appName("DataIngestionDynamic").getOrCreate()

    logger = get_logger()
    # Crear el handler web que escriba en app.logs
    web_handler = WebLogHandler(app)
    formatter = logger.handlers[0].formatter if logger.handlers else None
    if formatter:
        web_handler.setFormatter(formatter)
    else:
        # Si no hay ningún handler, define uno básico
        from logging import Formatter
        web_handler.setFormatter(Formatter('[%(asctime)s] %(levelname)s - %(message)s'))
    logger.addHandler(web_handler)
    logger.info("Sesión Spark iniciada")

    # Registrar Blueprints
    from app.routes.main_routes import main_bp
    app.register_blueprint(main_bp)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        app.spark.stop()
    
    return app
