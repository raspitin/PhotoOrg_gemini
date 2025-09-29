import sqlite3
import logging
import threading

# Configura un logger specifico per questo modulo
log = logging.getLogger(__name__)

class DatabaseHandler:
    """
    Gestisce tutte le interazioni con il database SQLite in modo thread-safe.
    """
    def __init__(self, db_path):
        """
        Inizializza il gestore del database.

        Args:
            db_path (str): Percorso del file del database SQLite.
        """
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._setup_database()

    def _setup_database(self):
        """
        Crea la tabella principale per il tracciamento dei file se non esiste già.
        """
        try:
            with self._lock:
                cursor = self.connection.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS file_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_path TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL,
                        details TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.connection.commit()
        except sqlite3.Error as e:
            log.error(f"Errore durante la creazione della tabella del database: {e}")
            raise

    def log_item(self, file_path, status, details=""):
        """
        Registra un item (file o directory) nel database.
        Utilizza INSERT OR IGNORE per evitare errori su percorsi duplicati.

        Args:
            file_path (str): Percorso completo dell'item.
            status (str): Lo stato dell'item (es. 'processed', 'duplicate', 'file_unsupported').
            details (str, optional): Dettagli aggiuntivi. Defaults to "".
        """
        try:
            with self._lock:
                cursor = self.connection.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO file_log (file_path, status, details)
                    VALUES (?, ?, ?)
                """, (file_path, status, details))
                self.connection.commit()
        except sqlite3.Error as e:
            log.error(f"Impossibile scrivere nel database per il file {file_path}: {e}")

    def close(self):
        """
        Chiude la connessione al database.
        """
        if self.connection:
            self.connection.close()
            log.info("Connessione al database chiusa.")
