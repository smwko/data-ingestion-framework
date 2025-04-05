import json
import os 

def load_metadata(file_path=None):
    """
    Load metadata configuration from a JSON file.
    
    Args:
        file_path (str): Path to the JSON file. If None, defaults to 'metadata.json' in the current directory.
    
    Returns:
        dict: Parsed metadata configuration.
    """
    if file_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, 'metadata.json')
    
    with open(file_path, 'r') as f:
        config = json.load(f)
    
    return config