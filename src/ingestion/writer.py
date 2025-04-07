from src.utils.logger import get_logger

logger = get_logger()

class DataWriter:
    @staticmethod
    def write_dataframe(df, sink_conf):
        logger.info(f"Escribiendo datos en {sink_conf['paths']} con formato {sink_conf['format']} y modo {sink_conf['saveMode']}")
        for path in sink_conf["paths"]:
            df.write.mode(sink_conf["saveMode"]).format(sink_conf["format"]).save(path)
