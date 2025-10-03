# -*- coding: utf-8 -*-
"""
Photo and Video Organizer with Parallel Processing - v1.3.3 (Definitivo)
Organizza foto e video con processing parallelo multi-thread
"""

from typing import Dict, Optional, Any
from config.config_loader import ConfigLoader
from loggingSetup.logging_setup import LoggingSetup
from database.database_manager import DatabaseManager
from processing.file_processor import FileProcessor
from pathlib import Path
import sys
import logging
import shutil
import time
import os
import argparse
import traceback


def setup_minimal_logging():
    """
    Configura logging solo su file, console pulita per utente.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
        handlers=[]
    )


def validate_config(config: Dict[str, Any]) -> None:
    """
    Valida che la configurazione contenga tutte le chiavi richieste.
    """
    required_keys = [
        "source", "destination", "database", "log",
        "supported_extensions", "image_extensions", "video_extensions"
    ]
    
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Chiavi di configurazione mancanti: {', '.join(missing_keys)}")
    
    source_path = Path(config["source"]).resolve()
    if not source_path.exists():
        raise ValueError(f"Directory sorgente non trovata: {source_path}")
    
    if not source_path.is_dir():
        raise ValueError(f"Il percorso sorgente non è una directory: {source_path}")
    
    dest_path = Path(config["destination"]).resolve()
    if source_path == dest_path:
        raise ValueError(
            f"ERRORE CRITICO: Directory sorgente e destinazione sono identiche!\n"
            f"Sorgente: {source_path}\n"
            f"Destinazione: {dest_path}\n"
            f"Questo potrebbe causare perdita di dati. Specificare una destinazione diversa."
        )
    
    try:
        dest_path.relative_to(source_path)
        raise ValueError(
            f"ERRORE CRITICO: La destinazione è una sottodirectory della sorgente!\n"
            f"Sorgente: {source_path}\n"
            f"Destinazione: {dest_path}\n"
            f"Questo potrebbe causare perdita di dati o loop infiniti."
        )
    except ValueError as e:
        if "ERRORE CRITICO" in str(e):
            raise
        pass
    
    if dest_path.exists():
        try:
            source_path.relative_to(dest_path)
            raise ValueError(
                f"ERRORE CRITICO: La sorgente è una sottodirectory della destinazione!\n"
                f"Sorgente: {source_path}\n"
                f"Destinazione: {dest_path}\n"
                f"Configurazione non valida."
            )
        except ValueError as e:
            if "ERRORE CRITICO" in str(e):
                raise
            pass
    
    print("[SUCCESS] Configurazione validata con successo (incluse opzioni parallelismo e sicurezza path)")


def determine_worker_count(config: Dict[str, Any]) -> int:
    """
    Determina il numero ottimale di worker thread basandosi sulla configurazione.
    """
    parallel_config = config.get("parallel_processing", {})
    
    if parallel_config.get("max_workers") is not None:
        return parallel_config["max_workers"]
    
    cpu_count = os.cpu_count() or 4
    cpu_multiplier = parallel_config.get("cpu_multiplier", 2)
    max_limit = parallel_config.get("max_workers_limit", 16)
    
    optimal_workers = min(int(cpu_count * cpu_multiplier), max_limit)
    
    print(f"[CPU] Auto-detection worker: CPU={cpu_count}, multiplier={cpu_multiplier}, risultato={optimal_workers}")
    return optimal_workers


def create_destination_directory(dest_dir: Path, dry_run: bool = False) -> bool:
    """
    Gestisce la creazione della directory di destinazione con gestione errori robusta.
    """
    if dest_dir.exists():
        if not dest_dir.is_dir():
            logging.error(f"Il percorso di destinazione esiste ma non è una directory: {dest_dir}")
            print(f"[ERROR] '{dest_dir}' esiste ma non è una directory.")
            return False
        logging.info(f"Directory di destinazione già esistente: {dest_dir}")
        return True
    
    try:
        if dry_run:
            print(f"[DRY-RUN] Dovrei creare la directory: {dest_dir}")
            logging.info(f"[DRY-RUN] Simulazione creazione directory: {dest_dir}")
            return True
        
        response = input(f"La directory di destinazione '{dest_dir}' non esiste. Vuoi crearla? [s/N]: ").strip().lower()
        if response == "s":
            dest_dir.mkdir(parents=True, exist_ok=True)
            logging.info(f"Directory '{dest_dir}' creata con successo")
            print(f"[SUCCESS] Directory '{dest_dir}' creata con successo.")
            return True
        else:
            logging.info("Operazione annullata dall'utente")
            print("[INFO] Operazione annullata. Nessuna directory creata.")
            return False
            
    except Exception as e:
        logging.error(f"Errore imprevisto durante la creazione della directory '{dest_dir}': {e}")
        print(f"[ERROR] Errore imprevisto durante la creazione della directory '{dest_dir}': {e}")
        return False


def reset_environment(database_path: str, log_path: str, dest_dir: str) -> None:
    """
    Ripristina l'ambiente eliminando il database, i log e le directory di destinazione.
    """
    print("[RESET] ATTENZIONE: Procedura di Reset dell'Ambiente")
    print("Questa operazione eliminerà:")
    print(f"  - Database: {database_path}")
    print(f"  - File di log: {log_path}")
    print(f"  - Directory: {dest_dir}/PHOTO, {dest_dir}/VIDEO, etc.")
    
    try:
        response = input("Sei sicuro di voler procedere? [s/N]: ").strip().lower()
        if response != "s":
            print("[INFO] Reset annullato.")
            return
    except KeyboardInterrupt:
        print("\n[INFO] Reset interrotto.")
        return
    
    print("[RESET] Inizio procedura di reset dell'ambiente")
    
    paths_to_remove = [Path(database_path), Path(log_path)]
    folders_to_remove = ["PHOTO", "VIDEO", "PHOTO_DUPLICATES", "VIDEO_DUPLICATES", "ToReview"]
    for folder in folders_to_remove:
        paths_to_remove.append(Path(dest_dir) / folder)

    for path in paths_to_remove:
        try:
            if not path.exists():
                print(f"[INFO] Path non trovato, ignorato: {path}")
                continue
            if path.is_file():
                path.unlink()
                print(f"[SUCCESS] File eliminato: {path}")
            elif path.is_dir():
                shutil.rmtree(path)
                print(f"[SUCCESS] Cartella eliminata: {path}")
        except Exception as e:
            print(f"[ERROR] Errore eliminando '{path}': {e}")
    
    print("[SUCCESS] Reset dell'ambiente completato.")


def initialize_logging(config: Dict[str, Any]) -> None:
    """
    Inizializza il sistema di logging.
    """
    try:
        LoggingSetup.setup_logging(config["log"])
        logging.info("Sistema di logging completo inizializzato")
    except Exception as e:
        logging.error(f"Errore durante l'inizializzazione del logging: {e}")


def initialize_database(config: Dict[str, Any], dry_run: bool = False) -> Optional[DatabaseManager]:
    """
    Inizializza il database manager.
    """
    try:
        db_path = ":memory:" if dry_run else config["database"]
        db_manager = DatabaseManager(db_path)
        logging.info(f"Database manager inizializzato (path: {db_path})")
        return db_manager
    except Exception as e:
        logging.error(f"Errore durante l'inizializzazione del database: {e}")
        return None


def initialize_file_processor(config: Dict[str, Any], db_manager: DatabaseManager, dry_run: bool = False) -> Optional[FileProcessor]:
    """
    Inizializza il processore dei file.
    """
    try:
        max_workers = determine_worker_count(config)
        
        file_processor = FileProcessor(
            config=config,
            source_dir=config["source"],
            dest_dir=config["destination"],
            db_manager=db_manager,
            supported_extensions=config["supported_extensions"],
            image_extensions=config["image_extensions"],
            video_extensions=config["video_extensions"],
            photographic_prefixes=config.get("photographic_prefixes", []),
            exclude_hidden_dirs=config.get("exclude_hidden_dirs", True),
            exclude_patterns=config.get("exclude_patterns", []),
            max_workers=max_workers,
            dry_run=dry_run
        )
        logging.info(f"File processor inizializzato con {max_workers} worker (dry_run={dry_run})")
        return file_processor
    except Exception:
        print("\n--- ERRORE DETTAGLIATO DURANTE INIZIALIZZAZIONE FILE PROCESSOR ---")
        traceback.print_exc()
        print("-----------------------------------------------------------------\n")
        return None


def print_system_info(config: Dict[str, Any], worker_count: int, dry_run: bool = False, mode: str = 'fresh'):
    """
    Stampa informazioni di sistema e configurazione.
    """
    mode_str = " [MODALITÀ DRY-RUN]" if dry_run else ""
    print(f"\n[START] Photo and Video Organizer - v1.3.3{mode_str}")
    print("-" * 60)
    print(f"[INFO] Modalità di esecuzione: {mode.upper()}")
    print(f"[SYS] CPU disponibili: {os.cpu_count() or 'N/A'}")
    print(f"[THREADS] Worker thread: {worker_count}")
    print(f"[FILES] Directory sorgente: {config['source']}")
    print(f"[FILES] Directory destinazione: {config['destination']}")
    print(f"[DB] Database: {':memory:' if dry_run else config['database']}")
    print("-" * 60)


def generate_final_report(db_manager: DatabaseManager, processing_time: float, dry_run: bool = False):
    """
    Genera e mostra un report finale delle operazioni.
    """
    try:
        stats = db_manager.get_statistics()
        mode_str = " (DRY-RUN)" if dry_run else ""
        print(f"\n[STATS] REPORT FINALE{mode_str} - Processing completato in {processing_time:.2f} secondi")
        print("=" * 60)
        
        total = stats['general'].get('total_files', 0)
        processed = stats['general'].get('processed_files', 0)
        photos = stats['general'].get('photos', 0)
        videos = stats['general'].get('videos', 0)
        duplicates = stats['general'].get('duplicate_files', 0)
        unsupported = stats['general'].get('unsupported_files', 0)
        errors = stats['general'].get('error_files', 0)
        
        prefix = "SIMULAZIONE" if dry_run else "RISULTATO"
        
        print(f"[{prefix}] File totali analizzati: {total}")
        print(f"[{prefix}] File organizzati: {processed} (Foto: {photos}, Video: {videos})")
        print(f"[{prefix}] Duplicati gestiti: {duplicates}")
        print(f"[{prefix}] File non supportati: {unsupported}")
        if errors > 0:
            print(f"[ERROR] Errori riscontrati: {errors}")

        if stats.get('yearly'):
            print("\n[DATE] Distribuzione per anno:")
            for year, count in sorted(stats['yearly'].items(), reverse=True):
                print(f"   {year}: {count} file")
        
        if total > 0 and processing_time > 0:
            throughput = total / processing_time
            print(f"\n[PERF] Performance: {throughput:.1f} file/secondo")
        
        print("=" * 60)
        if dry_run:
            print("[DRY-RUN] Per eseguire realmente le operazioni, lancia il comando senza il flag --dry-run.")

    except Exception as e:
        print(f"[ERROR] Errore nella generazione del report finale: {e}")


def parse_arguments():
    """
    Gestisce il parsing degli argomenti da linea di comando.
    """
    parser = argparse.ArgumentParser(
        description="Photo and Video Organizer v1.3.3",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--reset", action="store_true", help="Reset completo dell'ambiente.")
    parser.add_argument("--dry-run", action="store_true", help="Simula le operazioni senza modifiche reali.")
    parser.add_argument("--mode", choices=['fresh', 'merge'], default='fresh', help="Modalità: 'fresh' o 'merge'.")
    parser.add_argument("--version", action="version", version="PhotoOrg v1.3.3")
    return parser.parse_args()


def main():
    """
    Funzione principale con gestione completa degli errori e supporto parallelismo.
    """
    setup_minimal_logging()
    args = parse_arguments()

    print("[START] Avvio Photo and Video Organizer")

    try:
        config = ConfigLoader.load_config()
        print("[SUCCESS] Configurazione caricata")
        validate_config(config)
    except Exception as e:
        print(f"[ERROR] Errore di configurazione: {e}")
        return

    initialize_logging(config)

    if args.reset:
        reset_environment(config["database"], config["log"], config["destination"])
        return
    
    worker_count = determine_worker_count(config)
    print_system_info(config, worker_count, args.dry_run, args.mode)

    if not create_destination_directory(Path(config["destination"]), args.dry_run):
        print("[ERROR] Operazione annullata. Impossibile procedere senza directory di destinazione.")
        return
    
    db_manager = initialize_database(config, args.dry_run)
    if db_manager is None:
        print("[ERROR] Errore critico: impossibile inizializzare il database.")
        return
    
    file_processor = initialize_file_processor(config, db_manager, args.dry_run)
    if file_processor is None:
        print("[ERROR] Errore critico: impossibile inizializzare il processore dei file.")
        return

    start_time = time.time()
    try:
        if args.mode == 'merge' and not args.dry_run:
            file_processor.pre_scan_destination()
        
        file_processor.scan_directory()
        
    except KeyboardInterrupt:
        print("\n[WARN] Operazione interrotta dall'utente.")
    except Exception as e:
        print(f"\n[ERROR] Errore imprevisto durante l'esecuzione: {e}")
        traceback.print_exc()
    finally:
        processing_time = time.time() - start_time
        generate_final_report(db_manager, processing_time, args.dry_run)
        if not args.dry_run and config.get("database_config", {}).get("vacuum_on_completion", True):
            print("[CLEAN] Ottimizzazione database...")
            db_manager.cleanup_database()
        print("[END] Esecuzione terminata.")


if __name__ == "__main__":
    main()