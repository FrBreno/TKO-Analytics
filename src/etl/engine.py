"""
ETL Engine para processamento automático de eventos, sessões e métricas.

Módulo responsável por orquestrar o pipeline ETL completo:
1. Leitura de eventos do banco de dados
2. Detecção de sessões
3. Cálculo de métricas
4. Persistência dos resultados
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

import structlog

from src.etl.session_detector import SessionDetector
from src.metrics.engine import MetricsEngine
from src.models.events import ExecEvent, MoveEvent, SelfEvent

logger = structlog.get_logger()


class ETLEngine:
    """
    Motor de processamento ETL para dados TKO Analytics.
    
    Gerencia o fluxo completo desde eventos brutos até métricas calculadas.
    """
    
    def __init__(
        self,
        db_path: str = 'src.db',
        session_timeout_minutes: int = 60
    ):
        """
        Inicializa o engine ETL.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
            session_timeout_minutes: Timeout para detecção de sessões
        """
        self.db_path = Path(db_path).resolve()
        self.session_timeout = session_timeout_minutes
        self.session_detector = SessionDetector(timeout_minutes=session_timeout_minutes)
        self.metrics_engine = MetricsEngine(session_timeout_minutes=session_timeout_minutes)
        
        logger.info(
            "ETLEngine initialized",
            db_path=str(self.db_path),
            session_timeout=session_timeout_minutes
        )
    
    def process_events(self) -> Dict[str, Any]:
        """
        Processa todos os eventos no banco de dados.
        
        Fluxo:
        1. Carrega eventos do banco
        2. Agrupa por estudante
        3. Detecta sessões
        4. Calcula métricas
        5. Persiste resultados
        
        Returns:
            Dict com estatísticas do processamento:
            - students_processed: número de estudantes
            - sessions_detected: número de sessões
            - metrics_calculated: número de métricas
            - duration_seconds: tempo de processamento
        """
        logger.info("Starting ETL processing")
        import time
        start_time = time.time()
        
        conn = sqlite3.connect(str(self.db_path))
        
        try:
            # Verificar eventos ANALYSIS disponíveis
            count_result = conn.execute('SELECT COUNT(*) FROM analysis_events').fetchone()
            event_count = count_result[0] if count_result else 0
            
            logger.info("Analysis events in database", count=event_count)
            
            if event_count == 0:
                logger.warning("No analysis events to process")
                return {
                    'students_processed': 0,
                    'sessions_detected': 0,
                    'metrics_calculated': 0,
                    'duration_seconds': 0
                }
            
            # Limpar sessões e métricas anteriores
            logger.info("Clearing previous sessions and metrics")
            conn.execute('DELETE FROM sessions')
            conn.execute('DELETE FROM metrics')
            conn.commit()
            
            # Carregar eventos de análise
            logger.info("Loading analysis events from database")
            rows = conn.execute("""
                SELECT 
                    student_hash, case_id, task_id, event_type,
                    timestamp, metadata
                FROM analysis_events
                ORDER BY student_hash, timestamp
            """).fetchall()
            
            # Agrupar por estudante
            logger.info("Grouping events by student")
            students_events = self._group_events_by_student(rows)
            
            logger.info(
                "Events grouped",
                students=len(students_events),
                total_events=len(rows)
            )
            
            # Processar cada estudante
            total_sessions = 0
            total_metrics = 0
            
            for student_hash, events_data in students_events.items():
                sessions_count, metrics_count = self._process_student(
                    conn=conn,
                    student_hash=student_hash,
                    events=events_data['events'],
                    case_id=events_data['case_id']
                )
                total_sessions += sessions_count
                total_metrics += metrics_count
            
            conn.commit()
            
            duration = time.time() - start_time
            
            result = {
                'students_processed': len(students_events),
                'sessions_detected': total_sessions,
                'metrics_calculated': total_metrics,
                'duration_seconds': round(duration, 2)
            }
            
            logger.info(
                "ETL processing complete",
                **result
            )
            
            return result
            
        finally:
            conn.close()
    
    def _group_events_by_student(self, rows: List[tuple]) -> Dict[str, Dict]:
        """
        Agrupa eventos por estudante e reconstrói objetos Pydantic.
        
        Args:
            rows: Linhas do banco (student_hash, case_id, task_id, event_type, timestamp, metadata)
        
        Returns:
            Dict mapeando student_hash para {'events': [...], 'case_id': str}
        """
        students_data = {}
        
        for row in rows:
            student_hash = row[0]
            case_id = row[1]
            task_id = row[2]
            event_type = row[3]
            timestamp = datetime.fromisoformat(row[4])
            
            # Parse metadata JSON
            metadata = {}
            if row[5]:
                try:
                    metadata = json.loads(row[5])
                except Exception as e:
                    logger.warning("Failed to parse metadata", error=str(e), raw=row[5])
            
            # Criar evento Pydantic
            event = self._create_event_object(
                event_type=event_type,
                timestamp=timestamp,
                task_id=task_id,
                metadata=metadata
            )
            
            if event is None:
                continue
            
            # Adicionar ao estudante
            if student_hash not in students_data:
                students_data[student_hash] = {
                    'events': [],
                    'case_id': case_id
                }
            
            students_data[student_hash]['events'].append(event)
        
        return students_data
    
    def _create_event_object(
        self,
        event_type: str,
        timestamp: datetime,
        task_id: str,
        metadata: dict
    ):
        """
        Cria objeto Pydantic de evento a partir dos dados do banco.
        
        Args:
            event_type: Tipo do evento (exec, move, self)
            timestamp: Timestamp do evento
            task_id: ID da tarefa
            metadata: Dados adicionais do evento
        
        Returns:
            ExecEvent, MoveEvent, SelfEvent ou None se tipo inválido
        """
        event_type_normalized = event_type.lower()
        
        try:
            if event_type_normalized in ('exec', 'execevent'):
                return ExecEvent(
                    timestamp=timestamp,
                    task_id=task_id,
                    mode=metadata.get('mode', 'FULL'),
                    rate=metadata.get('rate'),
                    size=metadata.get('size', 0),
                    error=metadata.get('error', 'NONE')
                )
            elif event_type_normalized in ('move', 'moveevent'):
                return MoveEvent(
                    timestamp=timestamp,
                    task_id=task_id,
                    action=metadata.get('action', 'DOWN')
                )
            elif event_type_normalized in ('self', 'selfevent'):
                return SelfEvent(
                    timestamp=timestamp,
                    task_id=task_id,
                    rate=metadata.get('rate', 0),
                    autonomy=metadata.get('autonomy'),
                    help_human=metadata.get('help_human'),
                    help_iagen=metadata.get('help_iagen'),
                    help_guide=metadata.get('help_guide'),
                    help_other=metadata.get('help_other'),
                    study_minutes=metadata.get('study_minutes')
                )
            else:
                logger.warning("Unknown event type", type=event_type)
                return None
                
        except Exception as e:
            logger.error(
                "Failed to create event object",
                event_type=event_type,
                error=str(e)
            )
            return None
    
    def _process_student(
        self,
        conn: sqlite3.Connection,
        student_hash: str,
        events: List,
        case_id: str
    ) -> tuple[int, int]:
        """
        Processa um estudante: detecta sessões e calcula métricas.
        
        Args:
            conn: Conexão com banco de dados
            student_hash: Hash do estudante
            events: Lista de eventos do estudante
            case_id: ID do caso
        
        Returns:
            Tupla (sessions_count, metrics_count)
        """
        # Detectar sessões
        sessions = self.session_detector.detect_sessions(
            events=events,
            case_id=case_id,
            student_id=student_hash
        )
        
        # Inserir sessões
        for session in sessions:
            conn.execute(
                """
                INSERT INTO sessions (
                    id, case_id, student_hash, task_id,
                    start_timestamp, end_timestamp, duration_seconds,
                    event_count, exec_count, move_count, self_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                session.to_db_row()
            )
        
        # Agrupar eventos por tarefa
        tasks_events = {}
        for event in events:
            if event.task_id not in tasks_events:
                tasks_events[event.task_id] = []
            tasks_events[event.task_id].append(event)
        
        # Calcular métricas por tarefa
        metrics_count = 0
        for task_id, task_events in tasks_events.items():
            task_sessions = [s for s in sessions if s.task_id == task_id]
            
            metrics = self.metrics_engine.compute_all_metrics(
                events=task_events,
                sessions=task_sessions,
                case_id=case_id,
                student_id=student_hash,
                task_id=task_id
            )
            
            # Inserir métricas
            for metric in metrics:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO metrics (
                        id, case_id, student_hash, task_id,
                        metric_name, metric_value, metadata, computed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    metric.to_db_row()
                )
                metrics_count += 1
        
        logger.debug(
            "Student processed",
            student=student_hash[:8],
            events=len(events),
            sessions=len(sessions),
            metrics=metrics_count
        )
        
        return len(sessions), metrics_count
