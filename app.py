import os
import json
import logging
from flask import Flask, render_template, redirect, url_for, flash, request
from pyspark.sql import SparkSession
from src.config import metadata
from src.engine.dataflow_engine import DataFlowEngine
from src.utils.logger import get_logger

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Cambia esto por una clave segura
app.logs = []  # Lista para almacenar logs

# Creamos un handler que guarda los logs en app.logs
class WebLogHandler(logging.Handler):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def emit(self, record):
        log_entry = self.format(record)
        self.app.logs.append(log_entry)

# Configuramos el logger para que también escriba en app.logs
logger = get_logger()
web_handler = WebLogHandler(app)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
web_handler.setFormatter(formatter)
logger.addHandler(web_handler)

# Crear una sesión Spark global (se puede cerrar cuando la app se detenga)
spark = SparkSession.builder.appName("DataIngestionDynamic").getOrCreate()
logger.info("Sesión Spark iniciada")

@app.route('/')
def index():
    # Mostramos los últimos 50 logs en la página
    logs = app.logs[-50:]
    return render_template('index.html', logs=logs)

@app.route('/run')
def run_pipeline():
    try:
        metadata_config = metadata.load_metadata()
        engine = DataFlowEngine(spark, metadata_config)
        engine.run()
        flash('Pipeline ejecutado correctamente.', 'success')
    except Exception as e:
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        logger.error(f'Error al ejecutar pipeline: {str(e)}')
    return redirect(url_for('index'))

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'json_file' not in request.files:
        flash('No se encontró el campo de archivo en la solicitud.', 'danger')
        return redirect(url_for('index'))
    file = request.files['json_file']
    if file.filename == '':
        flash('No se seleccionó ningún archivo.', 'danger')
        return redirect(url_for('index'))
    # Guardamos el archivo en la carpeta de entrada
    input_folder = os.path.join('data', 'input', 'events', 'person')
    os.makedirs(input_folder, exist_ok=True)
    file_path = os.path.join(input_folder, file.filename)
    file.save(file_path)
    flash(f'Archivo {file.filename} subido exitosamente.', 'success')
    logger.info(f'Archivo {file.filename} subido a {file_path}')
    return redirect(url_for('index'))

@app.route('/results')
def results():
    # Se asume que los archivos se han escrito en:
    # data/output/events/person/STANDARD_OK.json y STANDARD_KO.json
    ok_file = os.path.join('data', 'output', 'events', 'person', 'STANDARD_OK.json')
    ko_file = os.path.join('data', 'output', 'events', 'person', 'STANDARD_KO.json')
    standard_ok = []
    standard_ko = []
    
    if os.path.exists(ok_file):
        with open(ok_file, 'r', encoding='utf-8') as f:
            standard_ok = json.load(f)
    if os.path.exists(ko_file):
        with open(ko_file, 'r', encoding='utf-8') as f:
            standard_ko = json.load(f)
            
    return render_template('results.html', standard_ok=standard_ok, standard_ko=standard_ko)

if __name__ == '__main__':
    app.run(debug=True)
