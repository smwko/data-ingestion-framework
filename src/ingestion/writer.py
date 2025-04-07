import os
import tempfile
import glob
import shutil
from src.utils.logger import get_logger

logger = get_logger()

def write_single_file(df, final_file_path, format, saveMode):
    """
    Escribe el DataFrame en un único archivo con el nombre especificado.
    Se escribe en un directorio temporal, se coalescea a una sola partición,
    y luego se renombra el archivo generado al destino final.
    """
    # Crear un directorio temporal
    temp_dir = tempfile.mkdtemp()
    # Escribir el DataFrame en una única partición en el directorio temporal
    df.coalesce(1).write.mode(saveMode).format(format).save(temp_dir)
    # Buscar el archivo generado (part-*)
    part_files = glob.glob(os.path.join(temp_dir, "part-*"))
    if not part_files:
        raise Exception("No se encontró el archivo de salida en el directorio temporal.")
    part_file = part_files[0]
    # Asegurarse de que exista el directorio destino
    os.makedirs(os.path.dirname(final_file_path), exist_ok=True)
    # Mover (renombrar) el archivo a la ruta final
    shutil.move(part_file, final_file_path)
    # Limpiar el directorio temporal
    shutil.rmtree(temp_dir)
    logger.info(f"Archivo escrito en: {final_file_path}")

class DataWriter:
    @staticmethod
    def write_dataframe(df, sink_conf):
        # Mapeo de entradas a nombres de archivo sin ifs
        output_mapping = {
            "ok_with_date": "STANDARD_OK",
            "validation_ko": "STANDARD_KO"
        }
        output_filename = output_mapping.get(sink_conf["input"], sink_conf.get("name", ""))
        # Construir las rutas finales para cada path: el archivo se llamará, por ejemplo, STANDARD_OK.json
        final_file_paths = [os.path.join(path, output_filename + ".json") for path in sink_conf["paths"]]
        
        logger.info(f"Writing DataFrame to files: {final_file_paths} with format {sink_conf['format']} and saveMode {sink_conf['saveMode']}")
        # Escribir el DataFrame en cada uno de los destinos
        for final_file in final_file_paths:
            write_single_file(df, final_file, sink_conf["format"], sink_conf["saveMode"])
