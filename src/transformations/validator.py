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

    # Definir las funciones de validación sin if explícitos
    validation_mapping = {
        "notEmpty": lambda col: F.when(
            F.col(col).isNull() | (F.trim(F.col(col)) == ""),
            F.lit(f"ERR_{col.upper()}_EMPTY")
        ),
        "notNull": lambda col: F.when(
            F.col(col).isNull(),
            F.lit(f"ERR_{col.upper()}_NULL")
        )
    }

    # Crear una lista de expresiones para cada validación definida
    error_exprs = []
    for rule in params["validations"]:
        field = rule["field"]
        for v in rule["validations"]:
            error_expr = validation_mapping.get(v, lambda col: F.lit(None))(field)
            error_exprs.append(error_expr)

    # Combina las expresiones en un array y filtra los valores nulos usando F.filter
    df_validated = df.withColumn(
        "arraycoderrorbyfield",
        F.filter(F.array(*error_exprs), lambda x: x.isNotNull())
    )

    # Filtrar registros válidos e inválidos según el tamaño del array de errores
    context["validation_ok"] = df_validated.filter(F.size("arraycoderrorbyfield") == 0)\
                                            .drop("arraycoderrorbyfield")
    context["validation_ko"] = df_validated.filter(F.size("arraycoderrorbyfield") > 0)
    return context
