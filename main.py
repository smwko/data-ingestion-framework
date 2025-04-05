from pyspark.sql import SparkSession
from src.config import metadata
from src.engine.dataflow_engine import DataFlowEngine
from src.utils.logger import get_logger

logger = get_logger()

def main():
    spark = SparkSession.builder.appName("DataIngestionDynamic").getOrCreate()
    logger.info("Sesión Spark iniciada")

    # Cargar metadata desde el archivo JSON
    metadata_config = metadata.load_metadata()
    
    # Crear y ejecutar el engine del DataFlow
    engine = DataFlowEngine(spark, metadata_config)
    engine.run()

    logger.info("Prueba técnica completada. Cerrando sesión Spark.")
    spark.stop()

if __name__ == "__main__":
    main()
