# -*- coding: utf-8 -*-
"""
FileProcessor with Parallel Processing Support and Dry-Run Mode - v1.3.3 (Corretto)
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
from tqdm import tqdm

from processing.date_extractor import DateExtractor
from processing.hash_utils import HashUtils
from processing.file_utils import FileUtils


class FileProcessor:
    """
    Processore di file con supporto per elaborazione parallela multi-thread e modalità dry-run.
    Gestisce scansione, estrazione metadati, organizzazione e tracking database.
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        source_dir: str,
        dest_dir: str,
        db_manager,
        supported_extensions: List[str],
        image_extensions: List[str],
        video_extensions: List[str],
        photographic_prefixes: List[str] = None,
        exclude_hidden_dirs: bool = True,
        exclude_patterns: List[str] = None,
        max_workers: Optional[int] = None,
        dry_run: bool = False
    ):
        self.config = config
        self.source_dir = Path(source_dir)
        self.dest_dir = Path(dest_dir)
        self.db_manager = db_manager
        self.supported_extensions = [ext.lower() for ext in supported_extensions]
        self.image_extensions = [ext.lower() for ext in image_extensions]
        self.video_extensions = [ext.lower() for ext in video_extensions]
        self.photographic_prefixes = photographic_prefixes or []
        self.exclude_hidden_dirs = exclude_hidden_dirs
        self.exclude_patterns = exclude_patterns or []
        self.dry_run = dry_run
        
        self.max_workers = max_workers or self._detect_optimal_workers()
        
        self._db_lock = threading.Lock()
        self._connections = {}
        self._connection_lock = threading.Lock()
        
        self._progress_lock = threading.Lock()
        self._processed_count = 0
        self._error_count = 0
        self._duplicate_count = 0
        self._unsupported_count = 0
        
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'duplicate_files': 0,
            'error_files': 0,
            'unsupported_files': 0,
            'photos_organized': 0,
            'videos_organized': 0
        }
        
        mode_str = " (DRY-RUN)" if self.dry_run else ""
        logging.info(f"FileProcessor inizializzato con {self.max_workers} worker threads{mode_str}")

    def _detect_optimal_workers(self) -> int:
        cpu_count = os.cpu_count() or 4
        optimal_workers = min(cpu_count * 2, 16)
        logging.info(f"CPU rilevati: {cpu_count}, worker ottimali: {optimal_workers}")
        return optimal_workers

    def _get_thread_connection(self):
        thread_id = threading.get_ident()
        with self._connection_lock:
            if thread_id not in self._connections:
                self._connections[thread_id] = self.db_manager.create_db()
                logging.debug(f"Nuova connessione DB creata per thread {thread_id}")
            return self._connections[thread_id]

    def _cleanup_connections(self):
        with self._connection_lock:
            for thread_id, conn in self._connections.items():
                try:
                    conn.close()
                    logging.debug(f"Connessione DB chiusa per thread {thread_id}")
                except Exception as e:
                    logging.warning(f"Errore chiusura connessione thread {thread_id}: {e}")
            self._connections.clear()

    def pre_scan_destination(self):
        print("[MERGE] Inizio pre-scansione della directory di destinazione...")
        logging.info("Inizio pre-scansione della destinazione per la modalità merge.")
        
        try:
            dest_files = list(self.dest_dir.rglob("*"))
            files_to_hash = [f for f in dest_files if f.is_file() and self._is_supported_file(f)]
        except Exception as e:
            print(f"[ERROR] Impossibile leggere la directory di destinazione: {e}")
            logging.error(f"Errore durante la scansione della destinazione: {e}")
            return
        
        if not files_to_hash:
            print("[MERGE] Nessun file supportato trovato nella destinazione. Procedo normalmente.")
            logging.info("Nessun file trovato nella destinazione durante la pre-scansione.")
            return

        total_files_to_index = len(files_to_hash)
        print(f"[MERGE] Trovati {total_files_to_index} file esistenti da indicizzare con {self.max_workers} thread...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._hash_and_record_existing_file, file_path): file_path
                for file_path in files_to_hash
            }
            
            for future in tqdm(as_completed(future_to_file), total=len(files_to_hash), desc="Indicizzazione destinazione", unit="file"):
                file_path = future_to_file[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Errore durante l'indicizzazione del file di destinazione {file_path}: {e}")
        
        print(f"\n[MERGE] Pre-scansione della destinazione completata.")
        logging.info(f"Pre-scansione completata.")

    def _hash_and_record_existing_file(self, file_path: Path):
        try:
            conn = self._get_thread_connection()
            _, file_hash = HashUtils.compute_hash(file_path, self.config)
            
            if file_hash and not self._is_duplicate(file_hash, conn):
                media_type = "PHOTO" if file_path.suffix.lower() in self.image_extensions else "VIDEO"
                
                record = (
                    "N/A (existing file)", file_hash, "N/A", "N/A", media_type,
                    "EXISTING", str(file_path), file_path.name
                )
                with self._db_lock:
                    self.db_manager.insert_file(conn, record)
        except Exception as e:
            logging.warning(f"Impossibile indicizzare il file esistente {file_path}: {e}")
            raise

    def scan_directory(self):
        mode_str = " (modalità DRY-RUN)" if self.dry_run else ""
        logging.info(f"Inizio scansione directory{mode_str}: {self.source_dir}")
        
        files_to_process = self._collect_files()
        
        if not files_to_process:
            logging.warning("Nessun file trovato da processare")
            print("[WARN] Nessun file nuovo trovato nella directory sorgente.")
            return
        
        self.stats['total_files'] = len(files_to_process)
        logging.info(f"Trovati {len(files_to_process)} file da processare{mode_str}")
        
        print(f"[{'DRY-RUN' if self.dry_run else 'INFO'}] Trovati {len(files_to_process)} file da processare con {self.max_workers} thread paralleli")
        
        self._process_files_parallel(files_to_process)
        
        self._cleanup_connections()
        self._print_final_stats()

    def _collect_files(self) -> List[Path]:
        files_to_process = []
        print(f"[{'DRY-RUN' if self.dry_run else 'SCAN'}] Scansione directory sorgente in corso...")
        
        conn = self._get_thread_connection()
        try:
            for root_path in self.source_dir.rglob("*"):
                if self._should_skip_path(root_path):
                    continue
                
                if self._is_supported_file(root_path):
                    files_to_process.append(root_path)
                elif root_path.is_file():
                    with self._db_lock:
                        self.db_manager.insert_unprocessed_file(conn, str(root_path), "unsupported", f"Estensione non supportata: {root_path.suffix}")
        except Exception as e:
            logging.error(f"Errore durante la raccolta file: {e}")
            raise
        
        return files_to_process

    def _process_files_parallel(self, files: List[Path]):
        mode_str = " (DRY-RUN)" if self.dry_run else ""
        logging.info(f"Inizio processing parallelo{mode_str} con {self.max_workers} workers")
        
        print(f"[{'DRY-RUN' if self.dry_run else 'START'}] Inizio processing {len(files)} file con {self.max_workers} worker paralleli...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._process_single_file, file_path): file_path
                for file_path in files
            }
            
            pbar_desc = "Simulazione" if self.dry_run else "Elaborazione"
            for future in tqdm(as_completed(future_to_file), total=len(files), desc=pbar_desc, unit="file"):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    with self._progress_lock:
                        if result['status'] == 'duplicate':
                            self.stats['duplicate_files'] += 1
                        elif result['status'] in ['copied', 'simulated']:
                            if result['media_type'] == 'PHOTO':
                                self.stats['photos_organized'] += 1
                            else:
                                self.stats['videos_organized'] += 1
                            self.stats['processed_files'] += 1
                        elif result['status'] == 'error':
                            self.stats['error_files'] += 1
                except Exception as e:
                    logging.error(f"Errore processing {file_path}: {e}")
                    with self._progress_lock:
                        self.stats['error_files'] += 1
        
        print(f"\n[{'DRY-RUN' if self.dry_run else 'SUCCESS'}] Processing completato.")

    def _process_single_file(self, file_path: Path) -> Dict[str, Any]:
        try:
            conn = self._get_thread_connection()
            media_type = "PHOTO" if file_path.suffix.lower() in self.image_extensions else "VIDEO"
            _, file_hash = HashUtils.compute_hash(file_path, self.config)
            
            date_info = DateExtractor.extract_date(file_path, self.image_extensions, self.video_extensions)
            year, month = (date_info[0], date_info[1]) if date_info else ("Unknown", "Unknown")
            
            status = self._organize_file(file_path, media_type, year, month, file_hash, conn)
            
            return {'status': status, 'media_type': media_type, 'file_path': str(file_path)}
        except Exception as e:
            logging.error(f"Errore processing file {file_path}: {e}")
            with self._db_lock:
                conn = self._get_thread_connection()
                self.db_manager.insert_unprocessed_file(conn, str(file_path), "error", str(e))
            raise

    def _organize_file(self, file_path: Path, media_type: str, year: str, month: str, file_hash: str, conn) -> str:
        try:
            dest_dir = self.dest_dir / "ToReview" / media_type if year == "Unknown" else self.dest_dir / media_type / year / month
            is_duplicate = self._is_duplicate(file_hash, conn)
            
            status = "duplicate" if is_duplicate else ("simulated" if self.dry_run else "copied")
            
            if not self.dry_run:
                target_dir = self.dest_dir / f"{media_type}_DUPLICATES" if is_duplicate else dest_dir
                target_dir.mkdir(parents=True, exist_ok=True)
                final_path = FileUtils.safe_copy(file_path, target_dir, file_path.name)
            else:
                final_path = (self.dest_dir / f"{media_type}_DUPLICATES" if is_duplicate else dest_dir) / file_path.name

            with self._db_lock:
                record = (
                    str(file_path), file_hash, year, month, media_type,
                    status, str(final_path), final_path.name
                )
                self.db_manager.insert_file(conn, record)
            return status
        except Exception as e:
            logging.error(f"Errore organizzazione file {file_path}: {e}")
            with self._db_lock:
                self.db_manager.insert_unprocessed_file(conn, str(file_path), "error", str(e))
            return "error"

    def _is_duplicate(self, file_hash: str, conn) -> bool:
        if not file_hash: return False
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE hash = ?", (file_hash,))
        return cursor.fetchone()[0] > 0

    def _should_skip_path(self, path: Path) -> bool:
        return any(part.startswith('.') for part in path.parts if self.exclude_hidden_dirs) or \
               any(pattern in str(path) for pattern in self.exclude_patterns)

    def _is_supported_file(self, file_path: Path) -> bool:
        return file_path.is_file() and file_path.suffix.lower() in self.supported_extensions

    def _print_final_stats(self):
        mode_str = " (DRY-RUN)" if self.dry_run else ""
        logging.info(f"Statistiche finali{mode_str}: {self.stats}")
        
        print(f"\n[{'DRY-RUN' if self.dry_run else 'STATS'}] Riepilogo Elaborazione:")
        stats = self.stats
        print(f"[SUCCESS] File processati: {stats['processed_files']}")
        print(f"[PHOTO] Foto organizzate: {stats['photos_organized']}")
        print(f"[VIDEO] Video organizzati: {stats['videos_organized']}")
        print(f"[DUP] Duplicati gestiti: {stats['duplicate_files']}")
        if stats['error_files'] > 0:
            print(f"[ERROR] Errori: {stats['error_files']}")