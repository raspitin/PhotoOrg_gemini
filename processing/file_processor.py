# -*- coding: utf-8 -*-
"""
FileProcessor with Parallel Processing Support and Dry-Run Mode - v1.1.0
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
        config: Dict[str, Any], # <-- AGGIUNTA: Accetta il parametro config
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
        dry_run: bool = False  # NUOVO: flag per modalità simulazione
    ):
        """
        Inizializza il processore di file con configurazione parallela e dry-run.
        
        Args:
            source_dir: Directory sorgente da scansionare
            dest_dir: Directory di destinazione per l'organizzazione
            db_manager: Manager database per il tracking
            supported_extensions: Liste delle estensioni supportate
            image_extensions: Estensioni specifiche per immagini
            video_extensions: Estensioni specifiche per video
            photographic_prefixes: Prefissi per identificare file fotografici
            exclude_hidden_dirs: Se escludere directory nascoste
            exclude_patterns: Pattern aggiuntivi da escludere
            max_workers: Numero massimo di worker (auto-detect se None)
            dry_run: Se True, simula le operazioni senza modifiche reali
        """
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
        self._last_processed_file = ""
        
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
        """
        Rileva automaticamente il numero ottimale di worker thread.
        
        Returns:
            int: Numero ottimale di worker thread
        """
        cpu_count = os.cpu_count() or 4
        optimal_workers = min(cpu_count * 2, 16)
        logging.info(f"CPU rilevati: {cpu_count}, worker ottimali: {optimal_workers}")
        return optimal_workers

    def _get_thread_connection(self):
        """
        Ottiene una connessione database thread-safe per il thread corrente.
        
        Returns:
            sqlite3.Connection: Connessione database thread-safe
        """
        thread_id = threading.get_ident()
        
        with self._connection_lock:
            if thread_id not in self._connections:
                self._connections[thread_id] = self.db_manager.create_db()
                logging.debug(f"Nuova connessione DB creata per thread {thread_id}")
            
            return self._connections[thread_id]

    def _cleanup_connections(self):
        """Chiude tutte le connessioni database thread-safe."""
        with self._connection_lock:
            for thread_id, conn in self._connections.items():
                try:
                    conn.close()
                    logging.debug(f"Connessione DB chiusa per thread {thread_id}")
                except Exception as e:
                    logging.warning(f"Errore chiusura connessione thread {thread_id}: {e}")
            self._connections.clear()

    def scan_directory(self):
        """
        Scansiona la directory sorgente e processa tutti i file supportati in parallelo.
        """
        mode_str = " (modalità DRY-RUN)" if self.dry_run else ""
        logging.info(f"Inizio scansione directory{mode_str}: {self.source_dir}")
        
        files_to_process = self._collect_files()
        
        if not files_to_process:
            logging.warning("Nessun file trovato da processare")
            print("[WARN] Nessun file trovato nella directory sorgente.")
            return
        
        self.stats['total_files'] = len(files_to_process)
        logging.info(f"Trovati {len(files_to_process)} file da processare{mode_str}")
        
        if self.dry_run:
            print(f"[DRY-RUN] Trovati {len(files_to_process)} file da simulare con {self.max_workers} thread paralleli")
        else:
            print(f"[INFO] Trovati {len(files_to_process)} file da processare con {self.max_workers} thread paralleli")
        
        self._process_files_parallel(files_to_process)
        
        self._cleanup_connections()
        self._print_final_stats()

    def _collect_files(self) -> List[Path]:
        """
        Raccoglie tutti i file validi da processare con reporting dettagliato.
        
        Returns:
            List[Path]: Lista dei file da processare
        """
        files_to_process = []
        total_items = 0
        skipped_dirs = 0
        skipped_files = 0
        unsupported_files = 0
        
        mode_str = "[DRY-RUN] " if self.dry_run else ""
        print(f"{mode_str}[SCAN] Scansione directory in corso...")
        
        conn = self._get_thread_connection()

        try:
            all_items = list(self.source_dir.rglob("*"))
            total_items = len(all_items)
            
            print(f"{mode_str}[SCAN] Trovati {total_items} item totali da analizzare...")
            
            for root_path in all_items:
                if self._should_skip_path(root_path):
                    if root_path.is_dir():
                        skipped_dirs += 1
                    else:
                        skipped_files += 1
                    continue
                
                if root_path.is_file():
                    if self._is_supported_file(root_path):
                        files_to_process.append(root_path)
                    else:
                        unsupported_files += 1
                        with self._db_lock:
                            self.db_manager.insert_unprocessed_file(conn, str(root_path), "unsupported", f"Estensione non supportata: {root_path.suffix}")

        except Exception as e:
            logging.error(f"Errore durante la raccolta file: {e}")
            raise

        self.stats['unsupported_files'] = unsupported_files
        
        print(f"{mode_str}[STATS] Risultati scansione:")
        print(f"   [FILES] Item totali scansionati: {total_items}")
        print(f"   [SUCCESS] File supportati trovati: {len(files_to_process)}")
        print(f"   [SKIP] Directory ignorate: {skipped_dirs}")
        print(f"   [SKIP] File ignorati (pattern): {skipped_files}")
        print(f"   [SKIP] File non supportati: {unsupported_files}")
        
        if self.dry_run:
            print(f"   [SIMULATION] File da simulare: {len(files_to_process)}")
        else:
            print(f"   [TARGET] File da processare: {len(files_to_process)}")
        print()
        
        return files_to_process

    def _process_files_parallel(self, files: List[Path]):
        """
        Processa i file in parallelo con progress tracking elegante.
        """
        mode_str = " (DRY-RUN)" if self.dry_run else ""
        logging.info(f"Inizio processing parallelo{mode_str} con {self.max_workers} workers")
        
        if self.dry_run:
            print(f"[DRY-RUN] Inizio simulazione {len(files)} file con {self.max_workers} worker paralleli...")
        else:
            print(f"[START] Inizio processing {len(files)} file con {self.max_workers} worker paralleli...")
        
        completed = 0
        total = len(files)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._process_single_file, file_path): file_path
                for file_path in files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                
                try:
                    result = future.result()
                    
                    with self._progress_lock:
                        completed += 1
                        self._processed_count += 1
                        self._last_processed_file = file_path.name
                        
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
                    
                    percent = (completed / total) * 100
                    
                    if self.dry_run:
                        progress_msg = f"Simulazione: {completed}/{total} ({percent:.1f}%) - Ultimo file: {self._last_processed_file}"
                    else:
                        progress_msg = f"Elaborazione: {completed}/{total} ({percent:.1f}%) - Ultimo file: {self._last_processed_file}"
                    
                    sys.stdout.write(f"\r{progress_msg}")
                    sys.stdout.flush()
                        
                except Exception as e:
                    logging.error(f"Errore processing {file_path}: {e}")
                    
                    with self._progress_lock:
                        completed += 1
                        self._error_count += 1
                        self.stats['error_files'] += 1
                        self._last_processed_file = f"ERROR: {file_path.name}"
                    
                    percent = (completed / total) * 100
                    if self.dry_run:
                        progress_msg = f"Simulazione: {completed}/{total} ({percent:.1f}%) - Ultimo file: {self._last_processed_file}"
                    else:
                        progress_msg = f"Elaborazione: {completed}/{total} ({percent:.1f}%) - Ultimo file: {self._last_processed_file}"
                    
                    sys.stdout.write(f"\r{progress_msg}")
                    sys.stdout.flush()
        
        if self.dry_run:
            print(f"\n[DRY-RUN] Simulazione completata: {completed} file analizzati")
        else:
            print(f"\n[SUCCESS] Processing completato: {completed} file elaborati")

    def _process_single_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Processa un singolo file in modo thread-safe.
        
        Args:
            file_path: Path del file da processare
            
        Returns:
            Dict[str, Any]: Risultato del processing
        """
        try:
            conn = self._get_thread_connection()
            media_type = "PHOTO" if file_path.suffix.lower() in self.image_extensions else "VIDEO"
            
            _, file_hash = HashUtils.compute_hash(file_path, self.config)
            
            date_info = DateExtractor.extract_date(
                file_path,
                self.image_extensions,
                self.video_extensions
            )
            
            if date_info:
                year, month, _ = date_info
            else:
                year, month = "Unknown", "Unknown"
                logging.warning(f"Data non estratta per {file_path}")
            
            result = self._organize_file(
                file_path, media_type, year, month, file_hash, conn
            )
            
            return {
                'status': result,
                'media_type': media_type,
                'file_path': str(file_path)
            }
            
        except Exception as e:
            logging.error(f"Errore processing file {file_path}: {e}")
            with self._db_lock:
                conn = self._get_thread_connection()
                self.db_manager.insert_unprocessed_file(conn, str(file_path), "error", str(e))
            raise

    def _organize_file(
        self,
        file_path: Path,
        media_type: str,
        year: str,
        month: str,
        file_hash: str,
        conn
    ) -> str:
        """
        Organizza un singolo file nella struttura di destinazione (o simula in dry-run).
        
        Args:
            file_path: Path del file originale
            media_type: Tipo di media (PHOTO/VIDEO)
            year: Anno estratto
            month: Mese estratto  
            file_hash: Hash del file per duplicati
            conn: Connessione database thread-safe
            
        Returns:
            str: Status dell'operazione (copied/duplicate/error/simulated)
        """
        try:
            if year == "Unknown" or month == "Unknown":
                dest_dir = self.dest_dir / "ToReview" / media_type
            else:
                dest_dir = self.dest_dir / media_type / year / month
            
            is_duplicate = self._is_duplicate(file_hash, conn)
            
            if self.dry_run:
                if is_duplicate:
                    duplicate_dir = self.dest_dir / f"{media_type}_DUPLICATES"
                    final_path = duplicate_dir / file_path.name
                    status = "duplicate"
                    
                    logging.info(f"[DRY-RUN] Duplicato rilevato: {file_path} -> {final_path}")
                else:
                    final_path = dest_dir / file_path.name
                    status = "simulated"
                    
                    logging.info(f"[DRY-RUN] File da organizzare: {file_path} -> {final_path}")
            else:
                if is_duplicate:
                    duplicate_dir = self.dest_dir / f"{media_type}_DUPLICATES"
                    duplicate_dir.mkdir(parents=True, exist_ok=True)
                    final_path = FileUtils.safe_copy(file_path, duplicate_dir, file_path.name)
                    status = "duplicate"
                else:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    final_path = FileUtils.safe_copy(file_path, dest_dir, file_path.name)
                    status = "copied"
            
            with self._db_lock:
                record = (
                    str(file_path),
                    file_hash,
                    year,
                    month,
                    media_type,
                    status,
                    str(final_path),
                    final_path.name
                )
                self.db_manager.insert_file(conn, record)
            
            return status
            
        except Exception as e:
            logging.error(f"Errore organizzazione file {file_path}: {e}")
            with self._db_lock:
                self.db_manager.insert_unprocessed_file(conn, str(file_path), "error", str(e))
            return "error"

    def _is_duplicate(self, file_hash: str, conn) -> bool:
        """
        Controlla se un file è un duplicato basandosi sull'hash.
        
        Args:
            file_hash: Hash del file da controllare
            conn: Connessione database
            
        Returns:
            bool: True se è un duplicato
        """
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE hash = ?", (file_hash,))
        count = cursor.fetchone()[0]
        return count > 0

    def _should_skip_path(self, path: Path) -> bool:
        """
        Determina se un path deve essere saltato.
        
        Args:
            path: Path da verificare
            
        Returns:
            bool: True se deve essere saltato
        """
        if self.exclude_hidden_dirs and any(
            part.startswith('.') for part in path.parts
        ):
            return True
        
        for pattern in self.exclude_patterns:
            if pattern in str(path):
                return True
        
        return False

    def _is_supported_file(self, file_path: Path) -> bool:
        """
        Verifica se un file è supportato.
        
        Args:
            file_path: Path del file
            
        Returns:
            bool: True se supportato
        """
        return file_path.suffix.lower() in self.supported_extensions

    def _print_final_stats(self):
        """Stampa statistiche finali dell'elaborazione."""
        mode_str = " (DRY-RUN)" if self.dry_run else ""
        
        logging.info(f"Statistiche finali{mode_str}:")
        logging.info(f"   File totali trovati: {self.stats['total_files']}")
        logging.info(f"   File processati: {self.stats['processed_files']}")
        logging.info(f"   Foto organizzate: {self.stats['photos_organized']}")
        logging.info(f"   Video organizzati: {self.stats['videos_organized']}")
        logging.info(f"   File duplicati: {self.stats['duplicate_files']}")
        logging.info(f"   File non supportati: {self.stats['unsupported_files']}")
        logging.info(f"   Errori: {self.stats['error_files']}")
        
        if self.dry_run:
            print(f"\n[DRY-RUN] Riepilogo Simulazione:")
            print(f"[ANALYSIS] File analizzati: {self.stats['processed_files']}")
            print(f"[PHOTO] Foto da organizzare: {self.stats['photos_organized']}")
            print(f"[VIDEO] Video da organizzare: {self.stats['videos_organized']}")
            print(f"[DUP] Duplicati rilevati: {self.stats['duplicate_files']}")
            print(f"[UNSUPPORTED] File non supportati: {self.stats['unsupported_files']}")
            if self.stats['error_files'] > 0:
                print(f"[ERROR] Errori: {self.stats['error_files']}")
            print(f"[INFO] Simulazione parallela completata con {self.max_workers} worker")
            print(f"[INFO] Per eseguire realmente, rimuovi il flag --dry-run")
        else:
            print("\n[STATS] Riepilogo Elaborazione:")
            print(f"[SUCCESS] File processati: {self.stats['processed_files']}")
            print(f"[PHOTO] Foto organizzate: {self.stats['photos_organized']}")
            print(f"[VIDEO] Video organizzati: {self.stats['videos_organized']}")
            print(f"[DUP] Duplicati gestiti: {self.stats['duplicate_files']}")
            print(f"[UNSUPPORTED] File non supportati: {self.stats['unsupported_files']}")
            if self.stats['error_files'] > 0:
                print(f"[ERROR] Errori: {self.stats['error_files']}")
            print(f"[THREADS] Elaborazione parallela completata con {self.max_workers} worker")