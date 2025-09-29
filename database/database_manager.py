# -*- coding: utf-8 -*-
"""
Thread-Safe DatabaseManager - v1.1.0 con supporto dry-run
Manager database thread-safe per gestire operazioni SQLite concorrenti
"""

from typing import Dict, List, Optional, Any, Tuple
import sqlite3
import threading
import logging
from pathlib import Path


class DatabaseManager:
    """
    Manager database thread-safe per gestire operazioni SQLite concorrenti.
    Supporta connessioni multiple e operazioni atomiche, incluso database in memoria per dry-run.
    """
    
    def __init__(self, db_path: str):
        """
        Inizializza il database manager con supporto thread-safe.
        
        Args:
            db_path: Percorso del file database SQLite o ":memory:" per database in memoria
        """
        self.db_path = db_path
        self.is_memory_db = db_path == ":memory:"
        self._global_lock = threading.Lock()
        self._initialized = False
        self._memory_db_conn = None
        
        if not self.is_memory_db:
            db_file = Path(db_path)
            db_file.parent.mkdir(parents=True, exist_ok=True)
        
        if self.is_memory_db:
            with self._global_lock:
                self._memory_db_conn = self._create_memory_connection()
                self._initialize_schema(self._memory_db_conn)
                self._initialized = True
        
        logging.info(f"DatabaseManager inizializzato: {db_path}")

    def _create_memory_connection(self) -> sqlite3.Connection:
        """Crea connessione database in memoria."""
        conn = sqlite3.connect(
            ":memory:",
            check_same_thread=False,
            timeout=30.0,
            isolation_level='DEFERRED'
        )
        
        conn.execute("PRAGMA synchronous=MEMORY")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=10000")
        
        return conn

    def create_db(self) -> sqlite3.Connection:
        """Crea connessione database."""
        if self.is_memory_db:
            return self._memory_db_conn
        
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30.0,
            isolation_level='DEFERRED'
        )
        
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("PRAGMA cache_size=10000")
        
        with self._global_lock:
            if not self._initialized:
                self._initialize_schema(conn)
                self._initialized = True
        
        return conn

    def _initialize_schema(self, conn: sqlite3.Connection):
        """Inizializza schema database."""
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_path TEXT NOT NULL,
                hash TEXT,
                year TEXT,
                month TEXT,
                media_type TEXT,
                status TEXT,
                destination_path TEXT,
                final_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_size INTEGER,
                processing_thread TEXT,
                notes TEXT
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_hash 
            ON files(hash)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_media_type_year_month 
            ON files(media_type, year, month)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_status 
            ON files(status)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_original_path 
            ON files(original_path)
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_files INTEGER DEFAULT 0,
                processed_files INTEGER DEFAULT 0,
                duplicate_files INTEGER DEFAULT 0,
                error_files INTEGER DEFAULT 0,
                worker_threads INTEGER DEFAULT 1,
                session_duration REAL,
                completed_at TIMESTAMP
            )
        """)
        
        conn.commit()
        mode_str = " (in memoria)" if self.is_memory_db else ""
        logging.info(f"Schema database inizializzato{mode_str}")

    def insert_file(self, conn: sqlite3.Connection, record: Tuple[str, ...]):
        """Inserisce record file."""
        cursor = conn.cursor()
        
        try:
            extended_record = record + (threading.get_ident(),)
            
            cursor.execute("""
                INSERT INTO files (
                    original_path, hash, year, month, media_type, 
                    status, destination_path, final_name, processing_thread
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, extended_record)
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Errore inserimento database: {e}")
            conn.rollback()
            raise

    def insert_unprocessed_file(self, conn: sqlite3.Connection, original_path: str, status: str, notes: str):
        """Inserisce un record per un file non processato."""
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO files (original_path, status, notes, processing_thread)
                VALUES (?, ?, ?, ?)
            """, (original_path, status, notes, threading.get_ident()))
            conn.commit()
        except Exception as e:
            logging.error(f"Errore inserimento file non processato nel database: {e}")
            conn.rollback()
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """Recupera statistiche database."""
        try:
            if self.is_memory_db:
                if self._memory_db_conn is None:
                    return self._empty_stats()
                conn = self._memory_db_conn
            else:
                conn = sqlite3.connect(self.db_path)
            
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN status = 'copied' OR status = 'simulated' THEN 1 END) as processed,
                    COUNT(CASE WHEN status = 'duplicate' THEN 1 END) as duplicates,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) as errors,
                    COUNT(CASE WHEN status = 'unsupported' THEN 1 END) as unsupported,
                    COUNT(CASE WHEN media_type = 'PHOTO' THEN 1 END) as photos,
                    COUNT(CASE WHEN media_type = 'VIDEO' THEN 1 END) as videos
                FROM files
            """)
            
            general_stats = cursor.fetchone()
            
            cursor.execute("""
                SELECT year, COUNT(*) as count
                FROM files 
                WHERE year != 'Unknown' AND year IS NOT NULL
                GROUP BY year 
                ORDER BY year DESC
            """)
            
            yearly_stats = cursor.fetchall()
            
            if not self.is_memory_db:
                conn.close()
            
            return {
                'general': {
                    'total_files': general_stats[0],
                    'processed_files': general_stats[1],
                    'duplicate_files': general_stats[2],
                    'error_files': general_stats[3],
                    'unsupported_files': general_stats[4],
                    'photos': general_stats[5],
                    'videos': general_stats[6]
                },
                'yearly': dict(yearly_stats),
                'last_session': None
            }
        except Exception as e:
            logging.error(f"Errore recupero statistiche: {e}")
            return self._empty_stats()

    def _empty_stats(self):
        """Statistiche vuote."""
        return {
            'general': {
                'total_files': 0,
                'processed_files': 0,
                'duplicate_files': 0,
                'error_files': 0,
                'unsupported_files': 0,
                'photos': 0,
                'videos': 0
            },
            'yearly': {},
            'last_session': None
        }

    def cleanup_database(self):
        """Pulizia database (solo per file)."""
        if self.is_memory_db:
            return
            
        with self._global_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("VACUUM")
            cursor.execute("ANALYZE")
            conn.close()
            logging.info("Database ottimizzato")