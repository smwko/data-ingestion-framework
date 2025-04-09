from pyspark.sql import functions as F

from pyspark.sql import functions as F

def transform_validate_fields(transformation, context):
    """
    Aplica la validación de campos en el DataFrame indicado usando funciones nativas de Spark.
    La metadata debe tener en "params" la estructura:
      {
          "input": "nombre_del_dataframe",
          "validations": [
              {"field": "office", "validations": ["notEmpty"]},
              {"field": "age", "validations": ["notNull"]}
          ]
      }
    Se añade la columna "arraycoderrorbyfield" y se separan los registros en
    "validation_ok" y "validation_ko".
    """
    params = transformation["params"]
    df = context[params["input"]]

    # Mapeo de validaciones sin if explícitos
    validation_mapping = {
        "notEmpty": lambda col: F.when(
            F.col(col).isNull() | (F.trim(F.col(col)) == ""), F.lit(f"ERR_{col.upper()}_EMPTY")
        ),
        "notNull": lambda col: F.when(
            F.col(col).isNull(), F.lit(f"ERR_{col.upper()}_NULL")
        )
    }

    # Generar la lista de expresiones de error usando list comprehension
    error_exprs = [
        validation_mapping.get(v, lambda col: F.lit(None))(rule["field"])
        for rule in params["validations"]
        for v in rule["validations"]
    ]

    # Crear la columna con el array de errores filtrando valores nulos
    df_validated = df.withColumn(
        "arraycoderrorbyfield", F.filter(F.array(*error_exprs), lambda x: x.isNotNull())
    )

    # Agregar la columna "dt" con el timestamp actual a TODOS los registros
    df_validated = df_validated.withColumn("dt", F.current_timestamp())

    # Separar los registros en válidos e inválidos según el tamaño del array de errores
    context["validation_ok"] = df_validated.filter(F.size("arraycoderrorbyfield") == 0)\
                                            .drop("arraycoderrorbyfield")
    context["validation_ko"] = df_validated.filter(F.size("arraycoderrorbyfield") > 0)
    return context
