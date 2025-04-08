import os
from src.utils.logger import get_logger

logger = get_logger()

class DataReader:
    def __init__(self, spark):
        self.spark = spark

    def read_source(self, source_conf):
        # Log para ver el directorio actual
        current_dir = os.getcwd()
        logger.info(f"Directorio de trabajo actual: {current_dir}")

        # Extraer la carpeta base del path (por ejemplo, "data/input/insurance" de "data/input/insurance/*.json")
        folder = os.path.dirname(source_conf["path"])
        abs_folder = os.path.abspath(folder)
        logger.info(f"Ruta base calculada para el source: {abs_folder}")

        if not os.path.exists(folder):
            try:
                os.makedirs(folder, exist_ok=True)
                logger.info(f"Directorio creado: {abs_folder}")
            except Exception as e:
                logger.error(f"Error al crear el directorio {abs_folder}: {e}")
        else:
            logger.info(f"Directorio ya existe: {abs_folder}")

        logger.info(f"Leyendo datos desde {source_conf['path']} con formato {source_conf['format']}")
        return self.spark.read.format(source_conf["format"]).json(source_conf["path"])
