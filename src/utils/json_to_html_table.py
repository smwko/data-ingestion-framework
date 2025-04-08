import json
import os

def json_lines_to_html_table(json_filepath, table_id="result_table", caption=""):
    """
    Lee un archivo en formato JSON Lines y devuelve un string HTML de una tabla.
    """
    if not os.path.exists(json_filepath):
        return f"<p>Archivo no encontrado: {json_filepath}</p>"
    
    rows = []
    with open(json_filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    row = json.loads(line)
                    rows.append(row)
                except Exception as e:
                    # Ignora líneas que no se puedan parsear
                    continue

    if not rows:
        return "<p>No hay datos para mostrar.</p>"
    
    # Obtén las columnas de forma dinámica (las claves de la primera fila)
    columns = list(rows[0].keys())
    
    html = f'<table id="{table_id}" class="table table-striped">'
    if caption:
        html += f"<caption>{caption}</caption>"
    html += "<thead><tr>"
    for col in columns:
        html += f"<th>{col}</th>"
    html += "</tr></thead>"
    html += "<tbody>"
    for row in rows:
        html += "<tr>"
        for col in columns:
            value = row.get(col, "")
            # Si es la columna de errores y es una lista, únelos
            if col == "arraycoderrorbyfield" and isinstance(value, list):
                value = ", ".join(value)
            html += f"<td>{value}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    return html
