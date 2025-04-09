import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Rutas absolutas para evitar confusiones de contexto
INPUT_FOLDER = os.path.join(BASE_DIR, 'data', 'input', 'events', 'person')
METADATA_PATH = os.path.join(BASE_DIR, 'src', 'config', 'metadata.json')

# Rutas de salida para los archivos generados
ok_filepath = os.path.join('data', 'output', 'events', 'person', 'STANDARD_OK.json')
ko_filepath = os.path.join('data', 'output', 'discards', 'person', 'STANDARD_KO.json')

# Otras constantes globales (por ejemplo, secret key)
SECRET_KEY = 'SDGPRUEBATECNICA'
