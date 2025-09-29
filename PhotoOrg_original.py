# -*- coding: utf-8 -*-
"""
Photo and Video Organizer with Parallel Processing - v1.2.0
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


def setup_minimal_logging():
    """
    Configura logging solo su file, console pulita per utente.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
        handlers=[]  # Nessun handler console iniziale
    )


def validate_config(config: Dict[str, Any]) -> None:
    """
    Valida che la configurazione contenga tutte le chiavi richieste.
    Include validazione per configurazioni di parallelismo e controllo sorgente=destinazione.
    
    Args:
        config: Dizionario di configurazione da validare
        
    Raises:
        ValueError: Se mancano chiavi obbligatorie o i tipi non sono corretti
    """
    required_keys = [
        "source", "destination", "database", "log",
        "supported_extensions", "image_extensions", "video_extensions"
    ]
    
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Chiavi di configurazione mancanti: {', '.join(missing_keys)}")
    
    # Verifica che i path esistano o siano creabili
    source_path = Path(config["source"]).resolve()
    if not source_path.exists():
        raise ValueError(f"Directory sorgente non trovata: {source_path}")
    
    if not source_path.is_dir():
        raise ValueError(f"Il percorso sorgente non è una directory: {source_path}")
    
    # NUOVO: Controllo che sorgente e destinazione non coincidano
    dest_path = Path(config["destination"]).resolve()
    if source_path == dest_path:
        raise ValueError(
            f"ERRORE CRITICO: Directory sorgente e destinazione sono identiche!\n"
            f"Sorgente: {source_path}\n"
            f"Destinazione: {dest_path}\n"
            f"Questo potrebbe causare perdita di dati. Specificare una destinazione diversa."
        )
    
    # Controllo che la destinazione non sia una sottodirectory della sorgente
    try:
        dest_path.relative_to(source_path)
        raise ValueError(
            f"ERRORE CRITICO: La destinazione è una sottodirectory della sorgente!\n"
            f"Sorgente: {source_path}\n"
            f"Destinazione: {dest_path}\n"
            f"Questo potrebbe causare perdita di dati o loop infiniti."
        )
    except ValueError as e:
        # Se relative_to fallisce, è OK (destinazione non è dentro sorgente)
        if "ERRORE CRITICO" in str(e):
            raise  # Re-raise se è il nostro errore critico
        pass  # Altrimenti continua
    
    # Controllo che la sorgente non sia una sottodirectory della destinazione
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
    
    # Verifica che la directory sorgente sia accessibile in lettura
    try:
        list(source_path.iterdir())
    except PermissionError:
        raise ValueError(f"Permesso negato per accedere alla directory sorgente: {source_path}")
    except OSError as e:
        raise ValueError(f"Errore di accesso alla directory sorgente '{source_path}': {e}")
    
    # Verifica directory di destinazione se esiste
    if dest_path.exists():
        if not dest_path.is_dir():
            raise ValueError(f"Il percorso di destinazione esiste ma non è una directory: {dest_path}")
        
        try:
            test_file = dest_path / ".test_write_permission"
            test_file.touch()
            test_file.unlink()
        except PermissionError:
            raise ValueError(f"Permesso negato per scrivere nella directory di destinazione: {dest_path}")
        except OSError as e:
            raise ValueError(f"Errore di accesso alla directory di destinazione '{dest_path}': {e}")
    
    # Verifica che le estensioni siano liste
    for ext_key in ["supported_extensions", "image_extensions", "video_extensions"]:
        if not isinstance(config[ext_key], list):
            raise ValueError(f"'{ext_key}' deve essere una lista")
        if not config[ext_key]:
            raise ValueError(f"'{ext_key}' non può essere una lista vuota")
    
    # Verifica configurazioni di parallelismo
    if "parallel_processing" in config:
        parallel_config = config["parallel_processing"]
        if "max_workers" in parallel_config and parallel_config["max_workers"] is not None:
            if not isinstance(parallel_config["max_workers"], int) or parallel_config["max_workers"] < 1:
                raise ValueError("max_workers deve essere un intero positivo o null")
        
        if "cpu_multiplier" in parallel_config:
            if not isinstance(parallel_config["cpu_multiplier"], (int, float)) or parallel_config["cpu_multiplier"] <= 0:
                raise ValueError("cpu_multiplier deve essere un numero positivo")
    
    # Verifica che photographic_prefixes sia una lista (se presente)
    if "photographic_prefixes" in config and not isinstance(config["photographic_prefixes"], list):
        raise ValueError("'photographic_prefixes' deve essere una lista")
    
    # Verifica che exclude_patterns sia una lista (se presente)
    if "exclude_patterns" in config and not isinstance(config["exclude_patterns"], list):
        raise ValueError("'exclude_patterns' deve essere una lista")
    
    print("[SUCCESS] Configurazione validata con successo (incluse opzioni parallelismo e sicurezza path)")


def determine_worker_count(config: Dict[str, Any]) -> int:
    """
    Determina il numero ottimale di worker thread basandosi sulla configurazione.
    
    Args:
        config: Configurazione completa
        
    Returns:
        int: Numero ottimale di worker
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
    
    Args:
        dest_dir: Path della directory di destinazione
        dry_run: Se True, simula solo la creazione
        
    Returns:
        bool: True se la directory esiste o è stata creata, False altrimenti
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
            
    except PermissionError as e:
        logging.error(f"Permesso negato per creare la directory '{dest_dir}': {e}")
        print(f"[ERROR] Permesso negato per creare la directory '{dest_dir}': {e}")
        return False
    except OSError as e:
        logging.error(f"Errore del sistema operativo durante la creazione di '{dest_dir}': {e}")
        print(f"[ERROR] Errore del sistema durante la creazione della directory '{dest_dir}': {e}")
        return False
    except KeyboardInterrupt:
        logging.warning("Operazione interrotta dall'utente (Ctrl+C)")
        print("\n[WARN] Operazione interrotta dall'utente.")
        return False
    except Exception as e:
        logging.error(f"Errore imprevisto durante la creazione della directory '{dest_dir}': {e}")
        print(f"[ERROR] Errore imprevisto durante la creazione della directory '{dest_dir}': {e}")
        return False


def reset_environment(database_path: str, log_path: str, dest_dir: str) -> None:
    """
    Ripristina l'ambiente eliminando il database, i log e le directory di destinazione.
    
    Args:
        database_path: Percorso del file database
        log_path: Percorso del file di log
        dest_dir: Percorso della directory di destinazione
    """
    logging.info("Richiesta procedura di reset dell'ambiente")
    print("[RESET] ATTENZIONE: Procedura di Reset dell'Ambiente")
    print("Questa operazione eliminerà:")
    print(f"  - Database: {database_path}")
    print(f"  - File di log: {log_path}")
    print(f"  - Directory: {dest_dir}/PHOTO")
    print(f"  - Directory: {dest_dir}/VIDEO") 
    print(f"  - Directory: {dest_dir}/PHOTO_DUPLICATES")
    print(f"  - Directory: {dest_dir}/VIDEO_DUPLICATES")
    print(f"  - Directory: {dest_dir}/ToReview")
    print()
    
    try:
        response = input("Sei sicuro di voler procedere? [s/N]: ").strip().lower()
        if response != "s":
            logging.info("Reset annullato dall'utente")
            print("[INFO] Reset annullato.")
            return
    except KeyboardInterrupt:
        logging.info("Reset interrotto dall'utente (Ctrl+C)")
        print("\n[INFO] Reset interrotto.")
        return
    
    logging.info("Inizio procedura di reset dell'ambiente")
    print("[RESET] Inizio procedura di reset dell'ambiente")
    
    reset_success = True
    
    # Elimina il database
    db_path = Path(database_path)
    if db_path.exists():
        try:
            db_path.unlink()
            logging.info(f"Database eliminato: {db_path}")
            print(f"[SUCCESS] Database eliminato: {db_path}")
        except (PermissionError, OSError) as e:
            logging.error(f"Errore eliminando il database '{db_path}': {e}")
            print(f"[ERROR] Errore eliminando database '{db_path}': {e}")
            reset_success = False
    else:
        logging.info(f"Database non trovato: {db_path}")
        print(f"[INFO] Database non trovato: {db_path}")

    # Elimina il file di log
    log_file = Path(log_path)
    if log_file.exists():
        try:
            log_file.unlink()
            logging.info(f"File di log eliminato: {log_file}")
            print(f"[SUCCESS] File di log eliminato: {log_file}")
        except (PermissionError, OSError) as e:
            logging.error(f"Errore eliminando il log '{log_file}': {e}")
            print(f"[ERROR] Errore eliminando log '{log_file}': {e}")
            reset_success = False
    else:
        logging.info(f"File di log non trovato: {log_file}")
        print(f"[INFO] File di log non trovato: {log_file}")

    # Elimina le directory di destinazione
    dest_path = Path(dest_dir)
    folders_to_remove = ["PHOTO", "VIDEO", "PHOTO_DUPLICATES", "VIDEO_DUPLICATES", "ToReview"]
    
    for folder in folders_to_remove:
        folder_path = dest_path / folder
        if folder_path.exists():
            try:
                shutil.rmtree(folder_path)
                logging.info(f"Cartella eliminata: {folder_path}")
                print(f"[SUCCESS] Cartella eliminata: {folder_path}")
            except (PermissionError, OSError) as e:
                logging.error(f"Errore eliminando '{folder_path}': {e}")
                print(f"[ERROR] Errore eliminando '{folder_path}': {e}")
                reset_success = False
        else:
            logging.info(f"Cartella non trovata: {folder_path}")
            print(f"[INFO] Cartella non trovata: {folder_path}")
    
    if reset_success:
        logging.info("Reset dell'ambiente completato con successo")
        print("[SUCCESS] Reset dell'ambiente completato con successo")
    else:
        logging.warning("Reset dell'ambiente completato con alcuni errori.")
        print("[WARN] Reset dell'ambiente completato con alcuni errori.")


def initialize_logging(config: Dict[str, Any]) -> None:
    """
    Inizializza il sistema di logging usando la configurazione fornita.
    
    Args:
        config: Dizionario di configurazione
    """
    try:
        LoggingSetup.setup_logging(config["log"])
        logging.info("Sistema di logging completo inizializzato")
    except Exception as e:
        logging.error(f"Errore durante l'inizializzazione del logging completo: {e}")
        logging.info("Continuo con il logging di base")


def initialize_database(config: Dict[str, Any], dry_run: bool = False) -> Optional[DatabaseManager]:
    """
    Inizializza il database manager con gestione errori.
    
    Args:
        config: Dizionario di configurazione
        dry_run: Se True, usa database in memoria per simulazione
        
    Returns:
        DatabaseManager istanziato o None in caso di errore
    """
    try:
        if dry_run:
            # In modalità dry-run usa database in memoria
            db_manager = DatabaseManager(":memory:")
            logging.info("Database manager thread-safe inizializzato (modalità DRY-RUN - database in memoria)")
        else:
            db_manager = DatabaseManager(config["database"])
            logging.info("Database manager thread-safe inizializzato")
        return db_manager
    except Exception as e:
        logging.error(f"Errore durante l'inizializzazione del database: {e}")
        return None


def initialize_file_processor(config: Dict[str, Any], db_manager: DatabaseManager, dry_run: bool = False) -> Optional[FileProcessor]:
    """
    Inizializza il processore dei file con gestione errori e supporto parallelo.
    
    Args:
        config: Dizionario di configurazione
        db_manager: Istanza del database manager
        dry_run: Se True, attiva modalità simulazione
        
    Returns:
        FileProcessor istanziato o None in caso di errore
    """
    try:
        max_workers = determine_worker_count(config)
        
        file_processor = FileProcessor(
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
            dry_run=dry_run  # NUOVO: passa il flag dry_run
        )
        logging.info(f"File processor inizializzato con {max_workers} worker paralleli (dry_run={dry_run})")
        return file_processor
    except Exception as e:
        logging.error(f"Errore durante l'inizializzazione del file processor: {e}")
        return None


def print_system_info(config: Dict[str, Any], worker_count: int, dry_run: bool = False):
    """
    Stampa informazioni di sistema e configurazione.
    
    Args:
        config: Configurazione completa
        worker_count: Numero di worker configurati
        dry_run: Se True, indica modalità simulazione
    """
    parallel_enabled = config.get("parallel_processing", {}).get("enabled", True)
    cpu_count = os.cpu_count() or "N/A"
    
    mode_str = " [MODALITÀ DRY-RUN]" if dry_run else ""
    print(f"\n[START] Photo and Video Organizer - Processing Parallelo v1.2.0{mode_str}")
    
    if dry_run:
        print(f"[DRY-RUN] MODALITÀ SIMULAZIONE ATTIVA - Nessuna modifica reale ai file")
        print(f"[DRY-RUN] Database temporaneo in memoria")
        print(f"[DRY-RUN] I file saranno solo analizzati, non spostati")
        print("-" * 60)
    
    print(f"[SYS] CPU disponibili: {cpu_count}")
    print(f"[THREADS] Worker thread: {worker_count}")
    print(f"[INFO] Processing parallelo: {'ABILITATO' if parallel_enabled else 'DISABILITATO'}")
    print(f"[FILES] Directory sorgente: {config['source']}")
    print(f"[FILES] Directory destinazione: {config['destination']}")
    
    if not dry_run:
        print(f"[DB] Database: {config['database']}")
    else:
        print(f"[DB] Database: :memory: (simulazione)")
    
    print("-" * 60)


def generate_final_report(db_manager: DatabaseManager, processing_time: float, dry_run: bool = False):
    """
    Genera e mostra un report finale delle operazioni.
    
    Args:
        db_manager: Manager database
        processing_time: Tempo di processing in secondi
        dry_run: Se True, indica modalità simulazione
    """
    try:
        stats = db_manager.get_statistics()
        
        mode_str = " [DRY-RUN]" if dry_run else ""
        print(f"\n[STATS] REPORT FINALE{mode_str} - Processing completato in {processing_time:.2f} secondi")
        
        if dry_run:
            print("[DRY-RUN] ATTENZIONE: Nessuna modifica reale ai file è stata effettuata!")
        
        print("=" * 60)
        print(f"[FILES] File totali processati: {stats['general']['total_files']}")
        
        if dry_run:
            print(f"[SIMULAZIONE] File che sarebbero stati organizzati: {stats['general']['processed_files']}")
            print(f"[SIMULAZIONE] Foto da organizzare: {stats['general']['photos']}")
            print(f"[SIMULAZIONE] Video da organizzare: {stats['general']['videos']}")
            print(f"[SIMULAZIONE] Duplicati rilevati: {stats['general']['duplicate_files']}")
        else:
            print(f"[SUCCESS] File organizzati: {stats['general']['processed_files']}")
            print(f"[PHOTO] Foto: {stats['general']['photos']}")
            print(f"[VIDEO] Video: {stats['general']['videos']}")
            print(f"[DUP] Duplicati gestiti: {stats['general']['duplicate_files']}")
        
        if stats['general']['error_files'] > 0:
            print(f"[ERROR] Errori: {stats['general']['error_files']}")
        
        if stats['yearly']:
            print(f"\n[DATE] Distribuzione per anno:")
            for year, count in sorted(stats['yearly'].items(), reverse=True):
                print(f"   {year}: {count} file")
        
        if stats['general']['total_files'] > 0:
            throughput = stats['general']['total_files'] / processing_time
            print(f"\n[PERF] Performance: {throughput:.1f} file/secondo")
        
        print("=" * 60)
        
        if dry_run:
            print("[DRY-RUN] Per eseguire realmente le operazioni, rimuovi il flag --dry-run")
        
    except Exception as e:
        logging.error(f"Errore generazione report finale: {e}")
        print(f"[ERROR] Errore nella generazione del report: {e}")


def parse_arguments():
    """
    Gestisce il parsing degli argomenti da linea di comando.
    
    Returns:
        argparse.Namespace: Argomenti parsati
    """
    parser = argparse.ArgumentParser(
        description="Photo and Video Organizer con Processing Parallelo v1.2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:
  python3 PhotoOrg.py              # Esegue l'organizzazione dei file
  python3 PhotoOrg.py --dry-run    # Simula l'organizzazione senza modifiche
  python3 PhotoOrg.py --reset      # Reset completo dell'ambiente
  
Per maggiori informazioni consulta README.md
        """
    )
    
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset completo dell'ambiente (database, log, directory)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Modalità simulazione: analizza i file senza effettuare modifiche reali"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="PhotoOrg v1.2.0"
    )
    
    return parser.parse_args()


def main():
    """
    Funzione principale con gestione completa degli errori e supporto parallelismo.
    """
    setup_minimal_logging()
    
    # Parse degli argomenti
    args = parse_arguments()
    
    print("[START] Avvio Photo and Video Organizer con Processing Parallelo")
    
    # Carica e valida la configurazione
    try:
        config = ConfigLoader.load_config()
        print("[SUCCESS] Configurazione caricata")
    except FileNotFoundError:
        logging.error("Il file di configurazione 'config.yaml' non è stato trovato")
        print("[ERROR] Il file di configurazione 'config.yaml' non è stato trovato.")
        return
    except Exception as e:
        logging.error(f"Errore durante il caricamento della configurazione: {e}")
        print(f"[ERROR] Errore durante il caricamento della configurazione: {e}")
        return
    
    # Valida la configurazione
    try:
        validate_config(config)
    except ValueError as e:
        logging.error(f"Configurazione non valida: {e}")
        print(f"[ERROR] Errore di configurazione: {e}")
        return
    except Exception as e:
        logging.error(f"Errore imprevisto durante la validazione: {e}")
        print(f"[ERROR] Errore durante la validazione della configurazione: {e}")
        return
    
    # Modalità di reset
    if args.reset:
        logging.info("Modalità reset attivata")
        initialize_logging(config)
        reset_environment(config["database"], config["log"], config["destination"])
        return
    
    # Flag dry-run
    dry_run = args.dry_run
    if dry_run:
        print("[DRY-RUN] Modalità simulazione attivata - nessuna modifica reale sarà effettuata")
        logging.info("Modalità DRY-RUN attivata")
    
    # Determina configurazione parallelismo
    worker_count = determine_worker_count(config)
    
    # Mostra info sistema
    print_system_info(config, worker_count, dry_run)
    
    # Inizializza il logging completo
    initialize_logging(config)
    
    # Verifica/crea la directory di destinazione
    dest_dir = Path(config["destination"])
    if not create_destination_directory(dest_dir, dry_run):
        logging.error("Impossibile procedere senza directory di destinazione")
        print("[ERROR] Operazione annullata. Impossibile procedere senza directory di destinazione.")
        return
    
    # Inizializza il database manager
    db_manager = initialize_database(config, dry_run)
    if db_manager is None:
        logging.error("Impossibile procedere senza database manager")
        print("[ERROR] Errore critico: impossibile inizializzare il database.")
        return
    
    # Inizializza il processore dei file
    file_processor = initialize_file_processor(config, db_manager, dry_run)
    if file_processor is None:
        logging.error("Impossibile procedere senza file processor")
        print("[ERROR] Errore critico: impossibile inizializzare il processore dei file.")
        return
    
    # Scansiona la directory di origine e processa i file
    start_time = time.time()
    try:
        if dry_run:
            logging.info("Inizio scansione e simulazione processing parallelo della directory sorgente")
        else:
            logging.info("Inizio scansione e processing parallelo della directory sorgente")
        
        file_processor.scan_directory()
        
        processing_time = time.time() - start_time
        logging.info(f"Processing completato in {processing_time:.2f} secondi")
        
        # Genera report finale
        generate_final_report(db_manager, processing_time, dry_run)
        
        # Ottimizza database se configurato (solo in modalità reale)
        if not dry_run and config.get("database_config", {}).get("vacuum_on_completion", True):
            print("[CLEAN] Ottimizzazione database...")
            db_manager.cleanup_database()
        
    except KeyboardInterrupt:
        processing_time = time.time() - start_time
        logging.warning(f"Operazione interrotta dall'utente dopo {processing_time:.2f} secondi")
        print(f"\n[WARN] Operazione interrotta dall'utente dopo {processing_time:.2f} secondi.")
        try:
            generate_final_report(db_manager, processing_time, dry_run)
        except:
            pass
    except PermissionError as e:
        logging.error(f"Permesso negato durante la scansione: {e}")
        print(f"[ERROR] Errore di permessi: {e}")
    except OSError as e:
        logging.error(f"Errore del sistema operativo durante la scansione: {e}")
        print(f"[ERROR] Errore del sistema: {e}")
    except Exception as e:
        processing_time = time.time() - start_time
        logging.error(f"Errore imprevisto durante la scansione: {e}")
        print(f"[ERROR] Errore durante la scansione della directory: {e}")
        try:
            generate_final_report(db_manager, processing_time, dry_run)
        except:
            pass


if __name__ == "__main__":
    main()