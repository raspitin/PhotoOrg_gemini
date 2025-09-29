import yaml
from pathlib import Path

class ConfigLoader:
    @staticmethod
    def load_config(file_path="config.yaml"):
        with open(file_path, "r") as f:
            return yaml.safe_load(f)

# Esempio di utilizzo
# config = ConfigLoader.load_config()