"""
Loader para carregar eventos TKO no banco de dados SQLite.

Este módulo implementa o carregamento batch de eventos validados
no banco de dados, com suporte a transações e tratamento de erros.
"""

import json
import uuid
import sqlite3
import hashlib
import structlog
from pathlib import Path
from typing import List, Optional, Dict, Any

from tko_analytics.models import BaseEvent, ExecEvent, MoveEvent, SelfEvent

logger = structlog.get_logger()


class LoadError(Exception):
    """Erro durante carregamento de eventos no banco."""
    pass


class SQLiteLoader:
    """
    Carregador de eventos para SQLite.
    
    Responsável por inserir eventos validados no banco de dados SQLite,
    mapeando modelos Pydantic para schema de tabelas.
    """
    
    def __init__(self, db_path: str, batch_size: int = 1000):
        """
        Inicializa o loader.
        
        Args:
            db_path: Caminho do banco de dados SQLite
            batch_size: Tamanho do batch para inserções (padrão: 1000)
            
        Raises:
            LoadError: Se banco não existe ou está inacessível
        """
        self.db_path = Path(db_path)
        self.batch_size = batch_size
        self.events_loaded = 0
        self.events_skipped = 0
        
        if not self.db_path.exists():
            raise LoadError(f"Database not found: {self.db_path}")
    
    def load_events(
        self,
        events: List[BaseEvent],
        student_id: str,
        case_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> int:
        """
        Carrega lista de eventos no banco de dados.
        
        Args:
            events: Lista de eventos validados
            student_id: ID do estudante (será hasheado para anonimização)
            case_id: ID do caso (opcional, gerado se None)
            session_id: ID da sessão (opcional)
            
        Returns:
            Número de eventos carregados com sucesso
            
        Raises:
            LoadError: Se houver erro fatal no carregamento
        """
        if not events:
            logger.warning("[SQLiteLoader.load_events] - load_events_empty", message="No events to load")
            return 0
        
        # Gera case_id se não fornecido
        if case_id is None:
            case_id = f"case_{uuid.uuid4().hex[:12]}"
        
        # Hash do student_id para anonimização
        student_hash = self._hash_student_id(student_id)
        
        logger.info("[SQLiteLoader.load_events] - load_events_started",
                   events=len(events),
                   case_id=case_id,
                   student_hash=student_hash[:8])
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        try:
            cursor = conn.cursor()
            
            # Carrega em batches
            for i in range(0, len(events), self.batch_size):
                batch = events[i:i + self.batch_size]
                self._load_batch(cursor, batch, case_id, student_hash, session_id)
            
            conn.commit()
            self.events_loaded = len(events)
            
            logger.info("[SQLiteLoader.load_events] - load_events_completed",
                       loaded=self.events_loaded,
                       skipped=self.events_skipped)
            
            return self.events_loaded
            
        except sqlite3.Error as e:
            conn.rollback()
            raise LoadError(f"Database error: {e}") from e
        finally:
            conn.close()
    
    def _load_batch(
        self,
        cursor: sqlite3.Cursor,
        batch: List[BaseEvent],
        case_id: str,
        student_hash: str,
        session_id: Optional[str]
    ) -> None:
        """Carrega um batch de eventos."""
        for event in batch:
            row = self._event_to_row(event, case_id, student_hash, session_id)
            
            try:
                cursor.execute("""
                    INSERT INTO events (
                        id, case_id, student_hash, task_id, activity,
                        event_type, timestamp, duration_seconds,
                        session_id, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
            except sqlite3.IntegrityError as e:
                # Duplicata (id já existe) - skippa silenciosamente
                logger.debug("[SQLiteLoader._load_batch] - event_skipped",
                            event_id=row[0],
                            reason=str(e))
                self.events_skipped += 1
    
    def _event_to_row(
        self,
        event: BaseEvent,
        case_id: str,
        student_hash: str,
        session_id: Optional[str]
    ) -> tuple:
        """
        Converte evento Pydantic em tupla para INSERT.
        
        Returns:
            Tupla: (id, case_id, student_hash, task_id, activity,
                   event_type, timestamp, duration_seconds, 
                   session_id, metadata_json)
        """
        event_id = self._generate_event_id(event, case_id)
        activity = self._map_activity(event)
        event_type = type(event).__name__
        timestamp = event.timestamp.isoformat()
        duration_seconds = None
        metadata = self._extract_metadata(event)
        metadata_json = json.dumps(metadata, ensure_ascii=False)
        
        return (
            event_id,
            case_id,
            student_hash,
            event.task_id,
            activity,
            event_type,
            timestamp,
            duration_seconds,
            session_id,
            metadata_json
        )
    
    def _generate_event_id(self, event: BaseEvent, case_id: str) -> str:
        """
        Gera ID único e determinístico para o evento.
        
        Usa hash SHA256 de (case_id + timestamp + task_id + tipo).
        """
        key = f"{case_id}|{event.timestamp.isoformat()}|{event.task_id}|{type(event).__name__}"
        hash_digest = hashlib.sha256(key.encode()).hexdigest()
        return f"evt_{hash_digest[:16]}"
    
    def _hash_student_id(self, student_id: str) -> str:
        """
        Hash do student_id para anonimização (SHA256).
        
        Preserva unicidade mas não permite reverter para ID original.
        """
        return hashlib.sha256(student_id.encode()).hexdigest()
    
    def _map_activity(self, event: BaseEvent) -> str:
        """
        Mapeia tipo de evento para activity name (pm4py convention).
        
        - ExecEvent → "test_execution"
        - MoveEvent → "task_navigation" 
        - SelfEvent → "self_assessment"
        """
        if isinstance(event, ExecEvent):
            return "test_execution"
        elif isinstance(event, MoveEvent):
            return "task_navigation"
        elif isinstance(event, SelfEvent):
            return "self_assessment"
        else:
            return "unknown_activity"
    
    def _extract_metadata(self, event: BaseEvent) -> Dict[str, Any]:
        """
        Extrai campos específicos do evento para metadata JSON.
        
        Mantém campos relevantes para análise posterior.
        """
        metadata: Dict[str, Any] = {
            "version": event.version
        }
        
        if isinstance(event, ExecEvent):
            metadata.update({
                "mode": event.mode,
                "rate": event.rate,
                "size": event.size,
                "error": event.error
            })
        elif isinstance(event, MoveEvent):
            metadata.update({
                "action": event.action
            })
        elif isinstance(event, SelfEvent):
            metadata.update({
                "rate": event.rate,
                "autonomy": event.autonomy,
                "help_sources": event.get_help_sources(),
                "has_help": event.has_any_help(),
                "study_minutes": event.study_minutes
            })
        
        return metadata
    
    def get_event_count(self, case_id: Optional[str] = None) -> int:
        """
        Retorna contagem de eventos no banco.
        
        Args:
            case_id: Se fornecido, conta apenas eventos deste caso
            
        Returns:
            Número de eventos
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if case_id:
            cursor.execute("SELECT COUNT(*) FROM events WHERE case_id = ?", (case_id,))
        else:
            cursor.execute("SELECT COUNT(*) FROM events")
        
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    
    def get_events(
        self,
        case_id: Optional[str] = None,
        task_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Recupera eventos do banco como dicionários.
        
        Args:
            case_id: Filtrar por case_id
            task_id: Filtrar por task_id
            limit: Máximo de eventos a retornar
            
        Returns:
            Lista de eventos como dicionários
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = "SELECT * FROM events WHERE 1=1"
        params = []
        
        if case_id:
            query += " AND case_id = ?"
            params.append(case_id)
        
        if task_id:
            query += " AND task_id = ?"
            params.append(task_id)
        
        query += " ORDER BY timestamp ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
