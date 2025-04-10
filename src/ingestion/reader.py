import os
from src.utils.logger import get_logger

# Se obtiene una instancia del logger configurado para la aplicación.
logger = get_logger()

class DataReader:
    def __init__(self, spark):
        """
        Inicializa el DataReader con la sesión Spark.
        
        Args:
            spark (SparkSession): La sesión Spark activa que se usará para leer datos.
        """
        self.spark = spark

    def read_source(self, source_conf):
        """
        Lee datos desde una fuente especificada en la metadata.
        
        La configuración 'source_conf' debe incluir, al menos:
            - "path": Ruta relativa o absoluta donde se encuentran los archivos JSON.
            - "format": Formato de los archivos (por ejemplo, "json").
            
        Este método:
            1. Registra en el log el directorio de trabajo actual.
            2. Calcula la carpeta base a partir del "path" proporcionado.
            3. Se asegura de que dicha carpeta exista, creándola en caso contrario.
            4. Lee el archivo(s) usando Spark y lo retorna.
        
        Args:
            source_conf (dict): Configuración del source extraída de la metadata.
            
        Returns:
            DataFrame: Un DataFrame de Spark con los datos leídos.
        """
        # 1. Registrar el directorio de trabajo actual para depuración.
        current_dir = os.getcwd()
        logger.info(f"Directorio de trabajo actual: {current_dir}")

        # 2. Extraer la carpeta base del path.
        # Por ejemplo, si source_conf["path"] es "data/input/insurance/*.json", 
        # se obtiene "data/input/insurance"
        folder = os.path.dirname(source_conf["path"])
        abs_folder = os.path.abspath(folder)
        logger.info(f"Ruta base calculada para el source: {abs_folder}")

        # 3. Verificar si la carpeta existe; si no, crearla.
        if not os.path.exists(folder):
            try:
                os.makedirs(folder, exist_ok=True)
                logger.info(f"Directorio creado: {abs_folder}")
            except Exception as e:
                logger.error(f"Error al crear el directorio {abs_folder}: {e}")
        else:
            logger.info(f"Directorio ya existe: {abs_folder}")

        # 4. Registrar en el log la acción de lectura y la configuración usada.
        logger.info(f"Leyendo datos desde {source_conf['path']} con formato {source_conf['format']}")

        # 5. Leer los datos usando Spark y retornar el DataFrame resultante.
        return self.spark.read.format(source_conf["format"]).json(source_conf["path"])
