from src.utils.logger import get_logger

logger = get_logger()

class DataWriter:
    @staticmethod
    def write_dataframe(df, sink_conf):
        # Usar "saveMode" en lugar de "mode"
        logger.info(f"Writing DataFrame to sink: {sink_conf['paths']} with format {sink_conf['format']} and mode {sink_conf['saveMode']}")
        for path in sink_conf["paths"]:
            df.write.mode(sink_conf["saveMode"]).format(sink_conf["format"]).save(path)