"""
Session Detector - Agrupa eventos em sessões de trabalho.

Uma sessão é definida como sequência contínua de eventos em uma tarefa,
onde o gap temporal entre eventos consecutivos é menor que o timeout.
"""

import hashlib
import structlog
from typing import List, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..models.events import BaseEvent, ExecEvent, MoveEvent, SelfEvent

logger = structlog.get_logger()


@dataclass
class Session:
    """Representa uma sessão de trabalho em uma tarefa."""
    
    id: str
    case_id: str
    student_hash: str
    task_id: str
    start_timestamp: datetime
    end_timestamp: datetime
    duration_seconds: int
    event_count: int
    exec_count: int
    move_count: int
    self_count: int
    events: List[BaseEvent]
    
    def to_db_row(self) -> Tuple:
        """Converte sessão para tupla para inserção no SQLite."""
        return (
            self.id,
            self.case_id,
            self.student_hash,
            self.task_id,
            self.start_timestamp.isoformat(),
            self.end_timestamp.isoformat(),
            self.duration_seconds,
            self.event_count,
            self.exec_count,
            self.move_count,
            self.self_count
        )


class SessionError(Exception):
    """Erro durante detecção de sessões."""
    pass


class SessionDetector:
    """
    Detecta e agrupa eventos em sessões de trabalho.
    
    Uma sessão termina quando:
    1. Há gap maior que timeout_minutes entre eventos consecutivos
    2. A tarefa muda (task_id diferente)
    3. Acabam os eventos
    
    Args:
        timeout_minutes: Timeout em minutos para considerar nova sessão
    """
    
    def __init__(self, timeout_minutes: int = 30):
        """
        Inicializa detector de sessões.
        
        Args:
            timeout_minutes: Gap máximo entre eventos para mesma sessão
        """
        if timeout_minutes <= 0:
            raise ValueError("timeout_minutes must be positive")
        
        self.timeout_minutes = timeout_minutes
        self.timeout_delta = timedelta(minutes=timeout_minutes)
    
    def detect_sessions(
        self,
        events: List[BaseEvent],
        case_id: str,
        student_id: str
    ) -> List[Session]:
        """
        Detecta sessões a partir de lista de eventos.
        
        Args:
            events: Lista de eventos ordenados por timestamp
            case_id: ID do caso
            student_id: ID do estudante (será hasheado)
        
        Returns:
            Lista de sessões detectadas
        
        Raises:
            SessionError: Se eventos não estão ordenados ou há erros
        """
        if not events:
            logger.info("[SessionDetector.detect_sessions] -  no_events_to_process", case_id=case_id)
            return []
        
        # Valida ordenação
        self._validate_event_order(events)
        
        # Hash do student_id
        student_hash = self._hash_student_id(student_id)
        
        # Detecta sessões
        sessions = []
        current_session_events = []
        current_task_id = None
        
        logger.info(
            "[SessionDetector.detect_sessions] - session_detection_started",
            case_id=case_id,
            events=len(events),
            timeout_minutes=self.timeout_minutes
        )
        
        for i, event in enumerate(events):
            # Primeira iteração
            if not current_session_events:
                current_session_events.append(event)
                current_task_id = event.task_id
                continue
            
            last_event = current_session_events[-1]
            time_gap = event.timestamp - last_event.timestamp
            task_changed = event.task_id != current_task_id
            
            # Condições para nova sessão
            if time_gap > self.timeout_delta or task_changed:
                # Finaliza sessão atual
                session = self._create_session(
                    current_session_events,
                    case_id,
                    student_hash,
                    current_task_id
                )
                sessions.append(session)
                
                # Inicia nova sessão
                current_session_events = [event]
                current_task_id = event.task_id
            else:
                # Continua sessão atual
                current_session_events.append(event)
        
        # Finaliza última sessão
        if current_session_events:
            session = self._create_session(
                current_session_events,
                case_id,
                student_hash,
                current_task_id
            )
            sessions.append(session)
        
        logger.info(
            "[SessionDetector.detect_sessions] - session_detection_completed",
            case_id=case_id,
            sessions=len(sessions),
            total_events=len(events)
        )
        
        return sessions
    
    def _create_session(
        self,
        events: List[BaseEvent],
        case_id: str,
        student_hash: str,
        task_id: str
    ) -> Session:
        """
        Cria objeto Session a partir de lista de eventos.
        
        Args:
            events: Eventos da sessão
            case_id: ID do caso
            student_hash: Hash SHA256 do student_id
            task_id: ID da tarefa
        
        Returns:
            Session criada
        """
        if not events:
            raise SessionError("Cannot create session from empty event list")
        
        start_ts = events[0].timestamp
        end_ts = events[-1].timestamp
        duration = int((end_ts - start_ts).total_seconds())
        
        # Contadores por tipo
        exec_count = sum(1 for e in events if isinstance(e, ExecEvent))
        move_count = sum(1 for e in events if isinstance(e, MoveEvent))
        self_count = sum(1 for e in events if isinstance(e, SelfEvent))
        
        # ID determinístico
        session_id = self._generate_session_id(
            case_id,
            task_id,
            start_ts,
            end_ts
        )
        
        return Session(
            id=session_id,
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            duration_seconds=duration,
            event_count=len(events),
            exec_count=exec_count,
            move_count=move_count,
            self_count=self_count,
            events=events
        )
    
    def _generate_session_id(
        self,
        case_id: str,
        task_id: str,
        start_ts: datetime,
        end_ts: datetime
    ) -> str:
        """
        Gera ID determinístico para sessão.
        
        Args:
            case_id: ID do caso
            task_id: ID da tarefa
            start_ts: Timestamp inicial
            end_ts: Timestamp final
        
        Returns:
            Session ID (hash SHA256 truncado)
        """
        data = f"{case_id}|{task_id}|{start_ts.isoformat()}|{end_ts.isoformat()}"
        hash_obj = hashlib.sha256(data.encode('utf-8'))
        return hash_obj.hexdigest()[:16]
    
    def _hash_student_id(self, student_id: str) -> str:
        """
        Anonimiza student_id via SHA256.
        
        Args:
            student_id: ID original do estudante
        
        Returns:
            Hash SHA256 truncado (8 caracteres)
        """
        hash_obj = hashlib.sha256(student_id.encode('utf-8'))
        return hash_obj.hexdigest()[:8]
    
    def _validate_event_order(self, events: List[BaseEvent]) -> None:
        """
        Valida que eventos estão ordenados por timestamp.
        
        Args:
            events: Lista de eventos
        
        Raises:
            SessionError: Se eventos não estão ordenados
        """
        for i in range(len(events) - 1):
            if events[i].timestamp > events[i + 1].timestamp:
                raise SessionError(
                    f"Events not sorted: event {i} timestamp "
                    f"{events[i].timestamp} > event {i+1} timestamp "
                    f"{events[i+1].timestamp}"
                )
    
    def save_sessions(self, sessions: List[Session], db_path: str) -> int:
        """
        Persiste sessões no banco SQLite.
        
        Args:
            sessions: Lista de sessões detectadas
            db_path: Caminho do banco SQLite
        
        Returns:
            Número de sessões inseridas
        
        Raises:
            SessionError: Se houver erro na inserção
        """
        import sqlite3
        
        if not sessions:
            logger.info("[SessionDetector.save_sessions] - no_sessions_to_save")
            return 0
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Prepara dados
            rows = [session.to_db_row() for session in sessions]
            
            # Insert batch
            cursor.executemany(
                """
                INSERT OR IGNORE INTO sessions (
                    id, case_id, student_hash, task_id,
                    start_timestamp, end_timestamp, duration_seconds,
                    event_count, exec_count, move_count, self_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )
            
            inserted = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(
                "[SessionDetector.save_sessions] - sessions_saved",
                sessions=len(sessions),
                inserted=inserted
            )
            
            return inserted
        
        except sqlite3.Error as e:
            raise SessionError(f"Failed to save sessions: {e}")


def get_sessions_from_db(
    db_path: str,
    case_id: str = None,
    task_id: str = None,
    limit: int = None
) -> List[dict]:
    """
    Recupera sessões do banco SQLite.
    
    Args:
        db_path: Caminho do banco SQLite
        case_id: Filtro opcional por case_id
        task_id: Filtro opcional por task_id
        limit: Limite de resultados
    
    Returns:
        Lista de sessões como dicionários
    """
    import sqlite3
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM sessions WHERE 1=1"
    params = []
    
    if case_id:
        query += " AND case_id = ?"
        params.append(case_id)
    
    if task_id:
        query += " AND task_id = ?"
        params.append(task_id)
    
    query += " ORDER BY start_timestamp ASC"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]
