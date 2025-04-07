from src.utils.logger import get_logger

logger = get_logger()

class DataReader:
    def __init__(self, spark):
        self.spark = spark

    def read_source(self, source_conf):
        logger.info(f"Leyendo datos desde {source_conf['path']} con formato {source_conf['format']}")
        return self.spark.read.format(source_conf["format"]).json(source_conf["path"])
