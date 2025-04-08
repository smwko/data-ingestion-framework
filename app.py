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

# Ruta del archivo de metadata
METADATA_PATH = os.path.join('src', 'config', 'metadata.json')

@app.route('/')
def index():
    # Listar archivos en el directorio de entrada
    file_list = []
    if os.path.exists(INPUT_FOLDER):
        file_list = os.listdir(INPUT_FOLDER)
    
    # Obtener únicamente el nombre del archivo de metadata
    metadata_filename = "No se encontró metadata."
    abs_metadata_path = os.path.abspath(METADATA_PATH)
    logger.info(f"Buscando metadata en: {abs_metadata_path}")
    if os.path.exists(METADATA_PATH):
        metadata_filename = os.path.basename(METADATA_PATH)
        logger.info(f"Metadata encontrada: {metadata_filename}")
    else:
        logger.error(f"Archivo de metadata no encontrado: {abs_metadata_path}")
    
    # Mostrar los últimos 50 logs
    logs = app.logs[-50:]
    return render_template('index.html', logs=logs, file_list=file_list, metadata_filename=metadata_filename)


def get_dynamic_input_folder():
    """
    Lee el metadata y extrae la carpeta base del primer source.
    Se asume que el metadata tiene un path como "data/input/insurance/*.json" 
    y se devuelve "data/input/insurance".
    """
    try:
        md = __import__("src.config.metadata", fromlist=["load_metadata"]).load_metadata()
        # Se asume que se usa el primer dataflow y el primer source
        source_path = md["dataflows"][0]["sources"][0]["path"]
        # Retirar parte del patrón, por ejemplo, reemplazamos el wildcard y extraemos la carpeta
        # Una forma simple es usar dirname. Por ejemplo, si source_path es "data/input/insurance/*.json",
        # os.path.dirname(source_path) devolverá "data/input/insurance"
        folder = os.path.dirname(source_path)
        return folder
    except Exception as e:
        # En caso de error, se puede devolver un valor por defecto (por ejemplo, el valor actual usado en el código)
        print(f"Error al obtener la carpeta dinámica desde metadata: {e}")
        return None

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
    
    # Usar la carpeta dinámica obtenida del metadata
    dynamic_input_folder = get_dynamic_input_folder()
    if not dynamic_input_folder:
        flash('No se pudo determinar la carpeta de entrada desde el metadata.', 'danger')
        return redirect(url_for('index'))
    
    # Convertir a ruta absoluta para asegurar la ubicación correcta
    abs_dynamic_input_folder = os.path.abspath(dynamic_input_folder)
    os.makedirs(abs_dynamic_input_folder, exist_ok=True)
    file_path = os.path.join(abs_dynamic_input_folder, file.filename)
    
    try:
        file.save(file_path)
        flash(f'Archivo {file.filename} subido exitosamente.', 'success')
        logger.info(f'Archivo {file.filename} subido a {file_path}')
    except Exception as e:
        flash(f'Error al guardar el archivo {file.filename}: {str(e)}', 'danger')
        logger.error(f'Error al guardar el archivo {file.filename} en {file_path}: {e}')
        return redirect(url_for('index'))
    
    try:
        md = metadata.load_metadata()
        engine = DataFlowEngine(spark, md)
        engine.run()
        flash('Pipeline ejecutado correctamente tras la subida.', 'success')
    except Exception as e:
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        logger.error(f'Error al ejecutar pipeline: {str(e)}')
    
    return redirect(url_for('index'))


@app.route('/upload_metadata', methods=['POST'])
def upload_metadata():
    logger.info("Endpoint /upload_metadata llamado.")
    if 'metadata_file' not in request.files:
        flash('No se encontró el campo de archivo para metadata.', 'danger')
        logger.error("No se encontró 'metadata_file' en request.files.")
        return redirect(url_for('index'))
    file = request.files['metadata_file']
    logger.info(f"Archivo recibido: {file.filename}")
    if file.filename == '':
        flash('No se seleccionó ningún archivo de metadata.', 'danger')
        logger.error("El nombre del archivo de metadata está vacío.")
        return redirect(url_for('index'))
    metadata_folder = os.path.join('src', 'config')
    os.makedirs(metadata_folder, exist_ok=True)
    # Forzar la sustitución: eliminar el archivo existente si lo hay.
    if os.path.exists(METADATA_PATH):
        os.remove(METADATA_PATH)
        logger.info(f"Archivo de metadata existente eliminado: {os.path.abspath(METADATA_PATH)}")
    try:
        file.save(METADATA_PATH)
        flash('Archivo de metadata subido y reemplazado exitosamente.', 'success')
        logger.info(f"Metadata reemplazada en: {os.path.abspath(METADATA_PATH)}")
    except Exception as e:
        flash(f'Error al guardar el archivo de metadata: {str(e)}', 'danger')
        logger.error(f"Error al guardar metadata: {str(e)}")
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
