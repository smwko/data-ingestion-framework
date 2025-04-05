from src.ingestion.reader import DataReader
from src.ingestion.writer import DataWriter
from src.transformations.validator import transform_validate_fields
from src.transformations.add_fields import transform_add_fields
from src.utils.logger import get_logger

logger = get_logger()

# Registro de transformaciones dinámicas
TRANSFORMATION_REGISTRY = {
    "validate_fields": transform_validate_fields,
    "add_fields": transform_add_fields,
}

class DataFlowEngine:
    def __init__(self, spark, metadata_config):
        self.spark = spark
        self.metadata = metadata_config
        self.reader = DataReader(spark)
        self.writer = DataWriter()

    def run(self):
        # Iterar sobre cada dataflow definido en la metadata
        for dataflow in self.metadata.get("dataflows", []):
            logger.info(f"Iniciando dataflow: {dataflow['name']}")
            context = {}
            # 1. Lectura de fuentes
            for source in dataflow.get("sources", []):
                context[source["name"]] = self.reader.read_source(source)
            # 2. Ejecución dinámica de transformaciones
            for transformation in dataflow.get("transformations", []):
                trans_type = transformation["type"]
                func = TRANSFORMATION_REGISTRY.get(trans_type)
                if func:
                    context = func(transformation, context)
                else:
                    logger.error(f"Transformación no soportada: {trans_type}")
            # Log para depurar las claves generadas en el contexto
            logger.info("Claves en el contexto después de las transformaciones: " + str(list(context.keys())))
            # 3. Escritura en sinks
            for sink in dataflow.get("sinks", []):
                input_key = sink["input"]
                if input_key in context:
                    self.writer.write_dataframe(context[input_key], sink)
                else:
                    logger.error(f"No se encontró DataFrame para sink con input: {input_key}")
            logger.info(f"Dataflow {dataflow['name']} finalizado.")
