# File: src/transformations/validator.py

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf

# Registro de funciones de validación
VALIDATION_FUNCTIONS = {
    "notEmpty": lambda field, value: f"ERR_{field.upper()}_EMPTY" if value is None or str(value).strip() == "" else "",
    "notNull":  lambda field, value: f"ERR_{field.upper()}_NULL" if value is None or (isinstance(value, str) and value.strip() == "") else "",
}

def transform_validate_fields(transformation, context):
    """
    Aplica la validación de campos en el DataFrame indicado.
    La metadata debe tener en "params" la estructura:
      {
          "input": "nombre_del_dataframe",
          "validations": [
              {"field": "office", "validations": ["notEmpty"]},
              {"field": "age", "validations": ["notNull"]}
          ]
      }
    Se añade la columna "arraycoderrorbyfield" y se separan los registros en
    "validation_ok" y "validation_ko". Además, se agrega la columna "dt"
    a todos los registros (válidos y descartados), sin usar if en el engine.
    """
    params = transformation["params"]
    df = context[params["input"]]

    # Crear una estructura con todas las columnas para pasar al UDF
    df_struct = df.withColumn("row_struct", F.struct(*df.columns))
    validate_udf = udf(
        lambda row: [
            VALIDATION_FUNCTIONS[v](rule["field"], row[rule["field"]])
            for rule in params["validations"]
            for v in rule["validations"]
            if VALIDATION_FUNCTIONS[v](rule["field"], row[rule["field"]]) != ""
        ],
        ArrayType(StringType())
    )
    df_validated = df_struct.withColumn("arraycoderrorbyfield", validate_udf(F.col("row_struct"))).drop("row_struct")
    
    # Agregar la columna "dt" a TODOS los registros
    df_validated = df_validated.withColumn("dt", F.current_timestamp())
    
    # Separar los registros válidos e inválidos
    context["validation_ok"] = df_validated.filter(F.size("arraycoderrorbyfield") == 0).drop("arraycoderrorbyfield")
    context["validation_ko"] = df_validated.filter(F.size("arraycoderrorbyfield") > 0)
    return context
