# -*- coding: utf-8 -*-
"""
FileProcessor with Parallel Processing Support and Dry-Run Mode - v1.2.0 (con Modalità Merge)
Processore di file con supporto per elaborazione parallela multi-thread e modalità simulazione
"""

from typing import List, Tuple, Optional, Dict, Any
import os
import sys
import logging
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tqdm import tqdm  # MODIFICA: Importa la libreria tqdm

from processing.date_extractor import DateExtractor
from processing.hash_utils import HashUtils
from processing.file_utils import FileUtils


class FileProcessor:
    # ... (tutto il codice precedente rimane invariato fino a _process_files_parallel) ...

    def _process_files_parallel(self, files: List[Path]):
        """
        Processa i file in parallelo con una barra di avanzamento tqdm.
        """
        mode_str = " (DRY-RUN)" if self.dry_run else ""
        logging.info(f"Inizio processing parallelo{mode_str} con {self.max_workers} workers")
        
        if self.dry_run:
            print(f"[DRY-RUN] Inizio simulazione {len(files)} file con {self.max_workers} worker paralleli...")
        else:
            print(f"[START] Inizio processing {len(files)} file con {self.max_workers} worker paralleli...")
        
        # MODIFICA: Rimuoviamo la gestione manuale del progresso
        # completed = 0
        # total = len(files)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._process_single_file, file_path): file_path
                for file_path in files
            }
            
            # MODIFICA: Aggiungiamo il wrapper tqdm qui. Questo è tutto!
            pbar_desc = "Simulazione" if self.dry_run else "Elaborazione"
            for future in tqdm(as_completed(future_to_file), total=len(files), desc=pbar_desc, unit="file"):
                file_path = future_to_file[future]
                
                try:
                    result = future.result()
                    
                    # La logica interna per aggiornare le statistiche rimane la stessa
                    with self._progress_lock:
                        self._processed_count += 1
                        
                        if result['status'] == 'duplicate':
                            self._duplicate_count += 1
                            self.stats['duplicate_files'] += 1
                        elif result['status'] == 'copied' or result['status'] == 'simulated':
                            if result['media_type'] == 'PHOTO':
                                self.stats['photos_organized'] += 1
                            else:
                                self.stats['videos_organized'] += 1
                            self.stats['processed_files'] += 1
                        elif result['status'] == 'error':
                            self._error_count += 1
                            self.stats['error_files'] += 1

                        # OPZIONALE: Se vuoi comunque vedere l'ultimo file processato
                        # pbar.set_postfix_str(f"Ultimo: {file_path.name[:30]}") # Tronca il nome per pulizia

                except Exception as e:
                    logging.error(f"Errore processing {file_path}: {e}")
                    with self._progress_lock:
                        self._error_count += 1
                        self.stats['error_files'] += 1
        
        # MODIFICA: Rimuoviamo la stampa finale del progresso, gestita da tqdm
        if self.dry_run:
            print(f"\n[DRY-RUN] Simulazione completata.")
        else:
            print(f"\n[SUCCESS] Processing completato.")

    # ... (il resto del file rimane invariato) ...