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

from src.models import BaseEvent, ExecEvent, MoveEvent, SelfEvent

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
        # Garantir caminho absoluto
        self.db_path = Path(db_path).resolve()
        self.batch_size = batch_size
        self.events_loaded = 0
        self.events_skipped = 0
        
        # Criar banco se não existir
        if not self.db_path.exists():
            logger.warning("[SQLiteLoader.__init__] - Database not found, initializing",
                          db_path=str(self.db_path))
            from src.etl.init_db import init_database
            try:
                init_database(str(self.db_path))
                logger.info("[SQLiteLoader.__init__] - Database initialized",
                           db_path=str(self.db_path))
            except Exception as e:
                raise LoadError(f"Failed to initialize database {self.db_path}: {e}") from e
        
        logger.info("[SQLiteLoader.__init__] - Loader initialized",
                   db_path=str(self.db_path),
                   batch_size=batch_size)
    
    def load_events(
        self,
        events: List[BaseEvent],
        student_id: str,
        case_id: Optional[str] = None,
        session_id: Optional[str] = None,
        dataset_role: str = "analysis",
        student_name: Optional[str] = None
    ) -> int:
        """
        Carrega lista de eventos no banco de dados.
        
        Args:
            events: Lista de eventos validados
            student_id: ID do estudante (será hasheado para anonimização)
            case_id: ID do caso (opcional, gerado se None)
            session_id: ID da sessão (opcional)
            dataset_role: 'model' ou 'analysis' (padrão: 'analysis')
            student_name: Nome do estudante (opcional)
            
        Returns:
            Número de eventos carregados com sucesso
            
        Raises:
            LoadError: Se houver erro fatal no carregamento
        """
        if not events:
            logger.warning("[SQLiteLoader.load_events] - load_events_empty", message="No events to load")
            return 0
        
        # Valida dataset_role
        if dataset_role not in ("model", "analysis"):
            raise LoadError(f"Invalid dataset_role: {dataset_role}. Must be 'model' or 'analysis'")
        
        # Gera case_id se não fornecido
        if case_id is None:
            case_id = f"case_{uuid.uuid4().hex[:12]}"
        
        # Hash do student_id para anonimização
        student_hash = self._hash_student_id(student_id)
        
        # Extrai student_name do primeiro evento se não fornecido
        if student_name is None and events and hasattr(events[0], 'student_name'):
            student_name = events[0].student_name
        
        logger.info("[SQLiteLoader.load_events] - load_events_started",
                   events=len(events),
                   case_id=case_id,
                   student_hash=student_hash[:8],
                   student_name=student_name,
                   dataset_role=dataset_role,
                   db_path=str(self.db_path))
        
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA foreign_keys = ON")
        
        logger.debug("[SQLiteLoader.load_events] - Database connection established")
        
        try:
            cursor = conn.cursor()
            
            # Carrega em batches
            for i in range(0, len(events), self.batch_size):
                batch = events[i:i + self.batch_size]
                self._load_batch(cursor, batch, case_id, student_hash, student_name, session_id, dataset_role)
            
            conn.commit()
            self.events_loaded = len(events)
            
            logger.info("[SQLiteLoader.load_events] - load_events_completed",
                       loaded=self.events_loaded,
                       skipped=self.events_skipped,
                       dataset_role=dataset_role,
                       db_path=str(self.db_path))
            
            # Verificar se realmente foi persistido na tabela correta
            table_name = f"{dataset_role}_events"
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE case_id = ?", (case_id,))
            persisted_count = cursor.fetchone()[0]
            logger.info("[SQLiteLoader.load_events] - Verification check",
                       persisted=persisted_count,
                       expected=self.events_loaded,
                       table=table_name)
            
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
        student_name: Optional[str],
        session_id: Optional[str],
        dataset_role: str
    ) -> None:
        """Carrega um batch de eventos na tabela correta (model_events ou analysis_events)."""
        # Define tabela baseada no dataset_role
        table_name = f"{dataset_role}_events"
        
        for batch_index, event in enumerate(batch):
            row = self._event_to_row(event, case_id, student_hash, student_name, session_id, batch_index)
            
            try:
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        id, case_id, student_hash, student_name, task_id, activity,
                        event_type, timestamp, duration_seconds,
                        session_id, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        student_name: Optional[str],
        session_id: Optional[str],
        batch_index: int = 0
    ) -> tuple:
        """
        Converte evento Pydantic em tupla para INSERT.
        
        Returns:
            Tupla: (id, case_id, student_hash, student_name, task_id, activity,
                   event_type, timestamp, duration_seconds, 
                   session_id, metadata_json)
        """
        event_id = self._generate_event_id(event, case_id, batch_index)
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
            student_name,
            event.task_id,
            activity,
            event_type,
            timestamp,
            duration_seconds,
            session_id,
            metadata_json
        )
    
    def _generate_event_id(self, event: BaseEvent, case_id: str, batch_index: int = 0) -> str:
        """
        Gera ID único e determinístico para o evento.
        
        Usa hash SHA256 de campos base + campos específicos por tipo + batch_index.
        
        Args:
            event: Evento a ser identificado
            case_id: ID do caso
            batch_index: Posição do evento no batch (garante unicidade para duplicatas verdadeiras)
            
        Returns:
            ID único no formato 'evt_<16_chars_hex>'
        """
        # Campos base comuns a todos os eventos
        key_parts = [
            case_id,
            event.timestamp.isoformat(),
            event.task_id,
            type(event).__name__,
            str(batch_index)
        ]
        
        # Adiciona campos específicos por tipo de evento
        if isinstance(event, ExecEvent):
            # ExecEvent: mode, rate, size, error
            key_parts.extend([
                event.mode,
                str(event.rate if event.rate is not None else 'None'),
                str(event.size),
                event.error or 'NONE'
            ])
        elif isinstance(event, MoveEvent):
            # MoveEvent: action
            key_parts.append(event.action)
        elif isinstance(event, SelfEvent):
            # SelfEvent: rate, autonomy, todas as fontes de ajuda
            key_parts.extend([
                str(event.rate),
                str(event.autonomy if event.autonomy is not None else 'None'),
                event.help_human or '',
                event.help_iagen or '',
                event.help_guide or '',
                event.help_other or '',
                str(event.study_minutes if event.study_minutes is not None else 'None')
            ])
        
        # Gera hash SHA256 dos campos concatenados
        key = "|".join(str(p) for p in key_parts)
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
        Inclui modo/ação quando disponível para granularidade maior.
        
        - ExecEvent → "EXEC [mode]" (ex: "EXEC FULL", "EXEC FREE")
        - MoveEvent → "MOVE [action]" (ex: "MOVE PICK", "MOVE BACK")
        - SelfEvent → "SELF"
        """
        if isinstance(event, ExecEvent):
            base_name = "EXEC"
            if hasattr(event, 'mode') and event.mode:
                return f"{base_name} {event.mode}"
            return base_name
        elif isinstance(event, MoveEvent):
            base_name = "MOVE"
            if hasattr(event, 'action') and event.action:
                return f"{base_name} {event.action}"
            return base_name
        elif isinstance(event, SelfEvent):
            return "SELF"
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
