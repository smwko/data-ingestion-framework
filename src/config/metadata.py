import json
import os

def load_metadata(file_path=None):
    """
    Carga la configuración de metadata desde un archivo JSON.

    Args:
        file_path (str, opcional): Ruta al archivo JSON. Si es None, se usa 'metadata.json'
            ubicado en el mismo directorio que este módulo.

    Returns:
        dict: Configuración de metadata parseada desde el archivo JSON.
    """
    # Si no se especifica un file_path, se utiliza el archivo 'metadata.json'
    # ubicado en el mismo directorio que este archivo (load_metadata.py).
    if file_path is None:
        # Se obtiene el directorio absoluto donde se encuentra este módulo.
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Se construye la ruta completa al archivo 'metadata.json'
        file_path = os.path.join(base_dir, 'metadata.json')
    
    # Abrir el archivo en modo lectura
    with open(file_path, 'r') as f:
        # Cargar y parsear el contenido JSON en un diccionario
        config = json.load(f)
    
    # Retornar la configuración parseada
    return config
