from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
import os
from src.config import metadata  # Asumiendo que este módulo está en src/config
from src.engine.dataflow_engine import DataFlowEngine
from src.utils.logger import get_logger
from src.utils.json_to_html_table import json_lines_to_html_table  # Si lo usas
from config import INPUT_FOLDER, METADATA_PATH, ok_filepath, ko_filepath  # Importar rutas de archivos y carpetas

logger = get_logger()

main_bp = Blueprint('main_bp', __name__)

# Rutas del blueprint (Ejemplo: la página principal)

@main_bp.route('/')
def index():
    # Listar archivos en el directorio de entrada que ahora definimos como config.py
    from config import INPUT_FOLDER, METADATA_PATH # Importamos constantes globales
    file_list = os.listdir(INPUT_FOLDER) if os.path.exists(INPUT_FOLDER) else []
    metadata_filename = os.path.basename(METADATA_PATH) if os.path.exists(METADATA_PATH) else "No se encontró metadata."
    logs = main_bp.current_app.logs[-50:] if hasattr (main_bp, 'current_app') and main_bp.current_app.logs else []
    return render_template('index.html', logs = logs, file_list=file_list, metadata_filename=metadata_filename)    

@main_bp.route('/run')
def run_pipeline():
    try:
        # Accede a la sesión Spark activa desde current_app
        spark = current_app.spark
        md_config = metadata.load_metadata()
        engine = DataFlowEngine(spark, md_config)
        engine.run()
        flash('Pipeline ejecutado correctamente.', 'success')
    except Exception as e:
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        current_app.logger.exception("Error ejecutando el pipeline")
    return redirect(url_for('main_bp.index'))

  
@main_bp.route('/upload', methods=['POST'])
def upload_file():
    if 'json_file' not in request.files:
        flash('No se encontró el campo de archivo en la solicitud.', 'danger')
        return redirect(url_for('main_bp.index'))
    
    file = request.files['json_file']
    if file.filename == '':
        flash('No se seleccionó ningún archivo.', 'danger')
        return redirect(url_for('main_bp.index'))
    
    abs_input_folder = os.path.abspath(INPUT_FOLDER)
    os.makedirs(abs_input_folder, exist_ok=True)
    file_path = os.path.join(abs_input_folder, file.filename)
    
    try:
        file.save(file_path)
        flash(f'Archivo {file.filename} subido exitosamente.', 'success')
        logger.info(f'Archivo {file.filename} subido a {file_path}')
    except Exception as e:
        flash(f'Error al guardar el archivo {file.filename}: {str(e)}', 'danger')
        logger.error(f'Error al guardar el archivo {file.filename} en {file_path}: {e}')
        return redirect(url_for('main_bp.index'))
    
    try:
        md = metadata.load_metadata()
        # Utiliza current_app para acceder a la aplicación actual y su Spark session
        spark = current_app.spark  
        engine = DataFlowEngine(spark, md)
        engine.run()
        flash('Pipeline ejecutado correctamente tras la subida.', 'success')
    except Exception as e:
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        logger.error(f'Error al ejecutar pipeline: {str(e)}')
    
    return redirect(url_for('main_bp.index'))


@main_bp.route('/upload_metadata', methods=['POST'])
def upload_metadata():
    logger.info("Endpoint /upload_metadata llamado.")
    if 'metadata_file' not in request.files:
        flash('No se encontró el campo de archivo para metadata.', 'danger')
        logger.error("No se encontró 'metadata_file' en request.files.")
        return redirect(url_for('main_bp.index'))
    file = request.files['metadata_file']
    logger.info(f"Archivo recibido: {file.filename}")
    if file.filename == '':
        flash('No se seleccionó ningún archivo de metadata.', 'danger')
        logger.error("El nombre del archivo de metadata está vacío.")
        return redirect(url_for('main_bp.index'))
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
    return redirect(url_for('main_bp.index'))

@main_bp.route('/delete/<filename>')
def delete_file(filename):
    file_path = os.path.join(INPUT_FOLDER, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        flash(f'Archivo {filename} eliminado.', 'success')
        logger.info(f'Archivo {filename} eliminado.')
    else:
        flash(f'Archivo {filename} no encontrado.', 'danger')
    return redirect(url_for('main_bp.index'))

@main_bp.route('/delete_all')
def delete_all():
    if os.path.exists(INPUT_FOLDER):
        for f in os.listdir(INPUT_FOLDER):
            os.remove(os.path.join(INPUT_FOLDER, f))
        flash('Todos los archivos han sido eliminados.', 'success')
        logger.info('Todos los archivos han sido eliminados.')
    else:
        flash('No hay archivos para eliminar.', 'info')
    return redirect(url_for('main_bp.index'))

@main_bp.route('/results')
def results():
    ok_table = json_lines_to_html_table(ok_filepath, table_id="standard_ok_table", caption="Resultados STANDARD_OK")
    ko_table = json_lines_to_html_table(ko_filepath, table_id="standard_ko_table", caption="Resultados STANDARD_KO")
    return render_template('results.html', ok_table=ok_table, ko_table=ko_table)