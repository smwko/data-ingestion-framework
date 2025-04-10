from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
import os
from src.config import metadata  # Asumiendo que este módulo está en src/config
from src.engine.dataflow_engine import DataFlowEngine
from src.utils.logger import get_logger
from src.utils.json_to_html_table import json_lines_to_html_table  # Si lo usas
from config import INPUT_FOLDER, METADATA_PATH, ok_filepath, ko_filepath  # Importar rutas de archivos y carpetas

# Importamos el logger configurado en src/utils/logger.py
logger = get_logger()

# Creamos el blueprint, asignándole el nombre 'main_bp' y el módulo actual (__name__)
from flask import Blueprint
main_bp = Blueprint('main_bp', __name__)

# Importamos las funciones necesarias para manipular la aplicación y renderizar plantillas
from flask import current_app, render_template, redirect, url_for, flash
# Importamos las constantes globales definidas en config.py para las rutas de entrada y metadata
from config import INPUT_FOLDER, METADATA_PATH
# Importamos la metadata y el engine del pipeline para ejecutar el flujo de datos
from src.config import metadata
from src.engine.dataflow_engine import DataFlowEngine

############################
#   Definición de Rutas    #
############################

# Endpoint principal: Muestra la página de inicio con archivos cargados, metadata y logs.
@main_bp.route('/')
def index():
    # Se comprueba si INPUT_FOLDER existe y se listan los archivos contenidos, o se usa una lista vacía
    file_list = os.listdir(INPUT_FOLDER) if os.path.exists(INPUT_FOLDER) else []
    
    # Se obtiene el nombre del archivo de metadata (si existe), o un mensaje predeterminado
    metadata_filename = os.path.basename(METADATA_PATH) if os.path.exists(METADATA_PATH) else "No se encontró metadata."
    
    # Se obtienen los últimos 50 registros de log desde current_app.logs (si la propiedad existe)
    logs = current_app.logs[-50:] if hasattr(current_app, "logs") else []
    
    # Se renderiza la plantilla 'index.html' pasando las listas de archivos, logs y el nombre de metadata
    return render_template('index.html',
                           logs=logs,
                           file_list=file_list,
                           metadata_filename=metadata_filename)
    


# Endpoint para ejecutar el pipeline de ingesta, validación y transformación
@main_bp.route('/run')
def run_pipeline():
    try:
        # Accede a la sesión Spark activa desde current_app (se creó en create_app())
        spark = current_app.spark
        
        # Se carga la configuración de metadata (archivo JSON) mediante la función load_metadata()
        md_config = metadata.load_metadata()
        
        # Se crea una instancia del Pipeline Engine y se ejecuta el pipeline
        engine = DataFlowEngine(spark, md_config)
        engine.run()
        
        # Se muestra un mensaje de éxito en la página usando flash
        flash('Pipeline ejecutado correctamente.', 'success')
    except Exception as e:
        # En caso de error, se muestra un mensaje de fallo y se registra la excepción en el logger
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        current_app.logger.exception("Error ejecutando el pipeline")
    
    # Se redirige a la página principal (index) del blueprint
    return redirect(url_for('main_bp.index'))



@main_bp.route('/upload', methods=['POST'])
def upload_file():
    # Verificar que en la solicitud exista el campo 'json_file'
    if 'json_file' not in request.files:
        flash('No se encontró el campo de archivo en la solicitud.', 'danger')
        return redirect(url_for('main_bp.index'))
    # Extraer el archivo enviado
    file = request.files['json_file']
    # Verificar que el archivo tenga un nombre (evita archivos vacíos)
    if file.filename == '':
        flash('No se seleccionó ningún archivo.', 'danger')
        return redirect(url_for('main_bp.index'))
    # Convertir la carpeta de entrada (definida globalmente en config.py) a una ruta absoluta
    abs_input_folder = os.path.abspath(INPUT_FOLDER)
    # Asegurarse de que el directorio de entrada exista; si no, se crea
    os.makedirs(abs_input_folder, exist_ok=True)
    # Generar la ruta completa donde se guardará el archivo
    file_path = os.path.join(abs_input_folder, file.filename)
    try:
        # Guardar el archivo subido en la ruta especificada
        file.save(file_path)
        flash(f'Archivo {file.filename} subido exitosamente.', 'success')
        logger.info(f'Archivo {file.filename} subido a {file_path}')
    except Exception as e:
        # Si ocurre un error al guardar el archivo, se muestra un mensaje de error y se registra
        flash(f'Error al guardar el archivo {file.filename}: {str(e)}', 'danger')
        logger.error(f'Error al guardar el archivo {file.filename} en {file_path}: {e}')
        return redirect(url_for('main_bp.index'))
    try:
        # Cargar la metadata desde el archivo de configuración
        md = metadata.load_metadata()
        # Acceder a la sesión Spark activa almacenada en la aplicación usando current_app
        spark = current_app.spark  
        # Crear el Pipeline Engine con la sesión Spark y la metadata cargada
        engine = DataFlowEngine(spark, md)
        # Ejecutar el pipeline (ingesta, validación y transformación de datos)
        engine.run()
        flash('Pipeline ejecutado correctamente tras la subida.', 'success')
    except Exception as e:
        # Capturar y registrar cualquier error en la ejecución del pipeline
        flash(f'Error al ejecutar pipeline: {str(e)}', 'danger')
        logger.error(f'Error al ejecutar pipeline: {str(e)}')
    # Redirigir al usuario a la página principal tras procesar la solicitud
    return redirect(url_for('main_bp.index'))



@main_bp.route('/upload_metadata', methods=['POST'])
def upload_metadata():
    # Registrar en el log que se ha llamado a este endpoint
    logger.info("Endpoint /upload_metadata llamado.")
    
    # Verificar que el request incluya el campo 'metadata_file'
    if 'metadata_file' not in request.files:
        flash('No se encontró el campo de archivo para metadata.', 'danger')
        logger.error("No se encontró 'metadata_file' en request.files.")
        return redirect(url_for('main_bp.index'))
    
    # Extraer el archivo subido para metadata
    file = request.files['metadata_file']
    logger.info(f"Archivo recibido: {file.filename}")
    
    # Verificar que el archivo tenga un nombre (para evitar archivos sin nombre)
    if file.filename == '':
        flash('No se seleccionó ningún archivo de metadata.', 'danger')
        logger.error("El nombre del archivo de metadata está vacío.")
        return redirect(url_for('main_bp.index'))
    
    # Definir la carpeta de metadata (se asume que se encuentra en 'src/config')
    metadata_folder = os.path.join('src', 'config')
    # Asegurarse de que la carpeta exista, creándola si no es así
    os.makedirs(metadata_folder, exist_ok=True)
    
    # Forzar la sustitución: si ya existe un archivo de metadata en la ruta METADATA_PATH, eliminarlo
    if os.path.exists(METADATA_PATH):
        os.remove(METADATA_PATH)
        logger.info(f"Archivo de metadata existente eliminado: {os.path.abspath(METADATA_PATH)}")
    
    try:
        # Guardar el archivo de metadata subido en la ubicación definida por METADATA_PATH
        file.save(METADATA_PATH)
        flash('Archivo de metadata subido y reemplazado exitosamente.', 'success')
        logger.info(f"Metadata reemplazada en: {os.path.abspath(METADATA_PATH)}")
    except Exception as e:
        # Si ocurre algún error al guardar el archivo, se notifica al usuario y se registra el error
        flash(f'Error al guardar el archivo de metadata: {str(e)}', 'danger')
        logger.error(f"Error al guardar metadata: {str(e)}")
    
    # Redirecciona al usuario a la página principal del blueprint
    return redirect(url_for('main_bp.index'))



@main_bp.route('/delete/<filename>')
def delete_file(filename):
    # Construir la ruta completa del archivo a eliminar utilizando INPUT_FOLDER (definido en config.py)
    file_path = os.path.join(INPUT_FOLDER, filename)
    
    # Verificar si el archivo existe en la ruta indicada
    if os.path.exists(file_path):
        # Si existe, eliminar el archivo
        os.remove(file_path)
        # Informar al usuario mediante flash y registrar el evento en el logger
        flash(f'Archivo {filename} eliminado.', 'success')
        logger.info(f'Archivo {filename} eliminado.')
    else:
        # Si el archivo no existe, informar al usuario y registrar el error
        flash(f'Archivo {filename} no encontrado.', 'danger')
        logger.error(f'Archivo {filename} no encontrado.')
    
    # Redirigir al usuario a la página principal del blueprint
    return redirect(url_for('main_bp.index'))



@main_bp.route('/delete_all')
def delete_all():
    # Verificar si la carpeta de entrada definida en INPUT_FOLDER existe
    if os.path.exists(INPUT_FOLDER):
        # Iterar sobre cada archivo en la carpeta e intentar eliminarlo
        for f in os.listdir(INPUT_FOLDER):
            os.remove(os.path.join(INPUT_FOLDER, f))
        # Informar al usuario que se han eliminado todos los archivos y registrar la acción
        flash('Todos los archivos han sido eliminados.', 'success')
        logger.info('Todos los archivos han sido eliminados.')
    else:
        # Si la carpeta no existe, informar al usuario
        flash('No hay archivos para eliminar.', 'info')
    
    # Redirigir al usuario a la página principal del blueprint
    return redirect(url_for('main_bp.index'))



@main_bp.route('/results')
def results():
    # Convertir los archivos JSON resultantes a tablas HTML para mostrarlos en la interfaz
    # Se utilizan dos variables, ok_filepath y ko_filepath, que deben estar definidas globalmente (p.ej., en config.py o de forma similar)
    ok_table = json_lines_to_html_table(ok_filepath, table_id="standard_ok_table", caption="Resultados STANDARD_OK")
    ko_table = json_lines_to_html_table(ko_filepath, table_id="standard_ko_table", caption="Resultados STANDARD_KO")
    
    # Renderizar la plantilla 'results.html' pasando las tablas generadas para datos válidos y descartados
    return render_template('results.html', ok_table=ok_table, ko_table=ko_table)
