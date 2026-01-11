"""
Script para inicialização do banco de dados SQLite.

Cria o schema completo do banco de dados TKO Analytics.
"""

import os
import sqlite3
import structlog
from pathlib import Path

logger = structlog.get_logger()


def init_database(db_path: str = "./data/tko_analytics.db") -> None:
    """
    Inicializa banco de dados SQLite com schema completo.
    
    Args:
        db_path: Caminho do arquivo de banco de dados
        
    Raises:
        sqlite3.Error: Se houver erro na criação do banco
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Habilitar foreign keys e WAL mode
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode = WAL")
    
    # Schema SQLite
    cursor.executescript("""
        -- Events
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            activity TEXT NOT NULL,
            event_type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            duration_seconds INTEGER,
            session_id TEXT,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_events_case_timestamp ON events(case_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_student ON events(student_hash);
        CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id);
        CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
        CREATE INDEX IF NOT EXISTS idx_events_activity ON events(activity);
        
        -- Sessions
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            start_timestamp TEXT NOT NULL,
            end_timestamp TEXT NOT NULL,
            duration_seconds INTEGER NOT NULL,
            event_count INTEGER DEFAULT 0,
            exec_count INTEGER DEFAULT 0,
            move_count INTEGER DEFAULT 0,
            self_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_case ON sessions(case_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_student ON sessions(student_hash);
        CREATE INDEX IF NOT EXISTS idx_sessions_task ON sessions(task_id);
        
        -- Code Snapshots
        CREATE TABLE IF NOT EXISTS code_snapshots (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            line_count INTEGER NOT NULL,
            storage_key TEXT NOT NULL,
            compression TEXT DEFAULT 'gzip',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_snapshots_case_timestamp ON code_snapshots(case_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_snapshots_storage_key ON code_snapshots(storage_key);
        
        -- Code Patches
        CREATE TABLE IF NOT EXISTS code_patches (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            previous_snapshot_id TEXT,
            patch_text TEXT NOT NULL,
            line_count_delta INTEGER NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (previous_snapshot_id) REFERENCES code_snapshots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_patches_case_timestamp ON code_patches(case_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_patches_snapshot ON code_patches(previous_snapshot_id);
        
        -- Metrics
        CREATE TABLE IF NOT EXISTS metrics (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            metadata TEXT,
            computed_at TEXT DEFAULT (datetime('now')),
            UNIQUE(case_id, metric_name)
        );
        CREATE INDEX IF NOT EXISTS idx_metrics_student_metric ON metrics(student_hash, metric_name);
        CREATE INDEX IF NOT EXISTS idx_metrics_task_metric ON metrics(task_id, metric_name);
        
        -- Behavioral Patterns
        CREATE TABLE IF NOT EXISTS behavioral_patterns (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            task_id TEXT NOT NULL,
            pattern_type TEXT NOT NULL,
            confidence REAL NOT NULL,
            evidence TEXT NOT NULL,
            detected_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_patterns_case ON behavioral_patterns(case_id);
        CREATE INDEX IF NOT EXISTS idx_patterns_type ON behavioral_patterns(pattern_type);
        
        -- Validation Errors
        CREATE TABLE IF NOT EXISTS validation_errors (
            id TEXT PRIMARY KEY,
            source_file TEXT NOT NULL,
            line_number INTEGER,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            raw_data TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_validation_errors_file ON validation_errors(source_file);
        CREATE INDEX IF NOT EXISTS idx_validation_errors_type ON validation_errors(error_type);
        CREATE INDEX IF NOT EXISTS idx_validation_errors_created ON validation_errors(created_at DESC);
    """)
    
    conn.commit()
    conn.close()
    
    logger.info(
        "[init_database] -  Database inicializado com sucesso.",
        db_path=str(db_path),
        tables=[
            "events",
            "sessions", 
            "code_snapshots",
            "code_patches",
            "metrics",
            "behavioral_patterns",
            "validation_errors"
        ]
    )


if __name__ == "__main__":
    db_path = os.getenv("TKO_DB_PATH", "./data/tko_analytics.db")
    init_database(db_path)
