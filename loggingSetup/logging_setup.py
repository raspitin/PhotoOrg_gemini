import logging
from pathlib import Path

class LoggingSetup:
    @staticmethod
    def setup_logging(log_path):
        """Setup logging solo su file, rimuovendo eventuali handler console."""
        log_file = Path(log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Rimuovi TUTTI gli handler esistenti
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:  # Copia la lista per evitare modifiche durante iterazione
            root_logger.removeHandler(handler)
        
        # Configura SOLO file logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                # NESSUN StreamHandler = NESSUN output su console
            ],
            force=True  # Forza la riconfigurazione
        )