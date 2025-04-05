from pyspark.sql import functions as F

# Registro de funciones para agregar campos
FUNCTION_REGISTRY = {
    "current_timestamp": F.current_timestamp,
}

def transform_add_fields(transformation, context):
    """
    Adds fields to the DataFrame as defined in the metadata.
    The metadata must have the following structure in "params":
      {
          "input": "name_of_the_dataframe",
          "addFields": [
              {"name": "dt", "function": "current_timestamp"}
          ]
      }
    """
    params = transformation["params"]
    df = context[params["input"]]
    for field in params.get("addFields", []):
        func = FUNCTION_REGISTRY.get(field["function"], lambda: F.lit(None))
        df = df.withColumn(field["name"], func())
    # Guardar el DataFrame resultante en el contexto bajo el nombre de la transformación
    context[transformation["name"]] = df
    return context
