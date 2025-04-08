import os
import json
import logging
from flask import Flask, render_template, redirect, url_for, flash, request
from pyspark.sql import SparkSession
from src.config import metadata
from src.engine.dataflow_engine import DataFlowEngine
from src.utils.logger import get_logger
from src.utils.json_to_html_table import json_lines_to_html_table

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Cambia esto por una clave segura
app.logs = []  # Lista para almacenar logs

# Handler para capturar logs en la página web
class WebLogHandler(logging.Handler):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def emit(self, record):
        log_entry = self.format(record)
        self.app.logs.append(log_entry)

logger = get_logger()
web_handler = WebLogHandler(app)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
web_handler.setFormatter(formatter)
logger.addHandler(web_handler)

# Crear una sesión Spark global (se puede cerrar cuando la app se detenga)
spark = SparkSession.builder.appName("DataIngestionDynamic").getOrCreate()
logger.info("Sesión Spark iniciada")

# Ruta de la carpeta de entrada (donde se suben los archivos JSON)
INPUT_FOLDER = os.path.join('data', 'input', 'events', 'person')

@app.route('/')
def index():
    # Listar archivos en el directorio de entrada (si existe)
    file_list = []
    if os.path.exists(INPUT_FOLDER):
        file_list = os.listdir(INPUT_FOLDER)
    # Mostrar los últimos 50 logs
    logs = app.logs[-50:]
    return render_template('index.html', logs=logs, file_list=file_list)

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
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    file_path = os.path.join(INPUT_FOLDER, file.filename)
    file.save(file_path)
    flash(f'Archivo {file.filename} subido exitosamente.', 'success')
    logger.info(f'Archivo {file.filename} subido a {file_path}')
    
    # Ejecutar el pipeline automáticamente después de subir el archivo
    try:
        metadata_config = metadata.load_metadata()
        engine = DataFlowEngine(spark, metadata_config)
        engine.run()
        flash('Pipeline ejecutado correctamente tras la subida.', 'success')
    except Exception as e:
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        logger.error(f'Error al ejecutar pipeline: {str(e)}')
    
    return redirect(url_for('index'))

@app.route('/delete/<filename>')
def delete_file(filename):
    file_path = os.path.join(INPUT_FOLDER, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        flash(f'Archivo {filename} eliminado.', 'success')
        logger.info(f'Archivo {filename} eliminado.')
    else:
        flash(f'Archivo {filename} no encontrado.', 'danger')
    return redirect(url_for('index'))

@app.route('/delete_all')
def delete_all():
    if os.path.exists(INPUT_FOLDER):
        for f in os.listdir(INPUT_FOLDER):
            os.remove(os.path.join(INPUT_FOLDER, f))
        flash('Todos los archivos han sido eliminados.', 'success')
        logger.info('Todos los archivos han sido eliminados.')
    else:
        flash('No hay archivos para eliminar.', 'info')
    return redirect(url_for('index'))


@app.route('/results')
def results():
    ok_filepath = os.path.join('data', 'output', 'events', 'person', 'STANDARD_OK.json')
    ko_filepath = os.path.join('data', 'output', 'discards', 'person', 'STANDARD_KO.json')
    
    ok_table = json_lines_to_html_table(ok_filepath, table_id="standard_ok_table", caption="Resultados STANDARD_OK")
    ko_table = json_lines_to_html_table(ko_filepath, table_id="standard_ko_table", caption="Resultados STANDARD_KO")
    
    return render_template('results.html', ok_table=ok_table, ko_table=ko_table)


if __name__ == '__main__':
    app.run(debug=True)
