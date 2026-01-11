"""
Metrics Engine - Cálculo de métricas pedagógicas TKO.

Este módulo implementa as métricas definidas em docs/METRICS.md.
"""

import hashlib
import json
import sqlite3
import structlog
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

from ..models.events import BaseEvent, ExecEvent, MoveEvent, SelfEvent
from ..etl.session_detector import Session

logger = structlog.get_logger()


@dataclass
class MetricResult:
    """Resultado de cálculo de métrica."""
    
    id: str
    case_id: str
    student_hash: str
    task_id: str
    metric_name: str
    metric_value: float
    metadata: Optional[Dict[str, Any]] = None
    computed_at: Optional[datetime] = None
    
    def to_db_row(self) -> tuple:
        """Converte para tupla para inserção no SQLite."""
        metadata_json = json.dumps(self.metadata) if self.metadata else None
        computed_at_str = (
            self.computed_at.isoformat()
            if self.computed_at
            else datetime.now().isoformat()
        )
        
        return (
            self.id,
            self.case_id,
            self.student_hash,
            self.task_id,
            self.metric_name,
            self.metric_value,
            metadata_json,
            computed_at_str
        )


class MetricsError(Exception):
    """Erro durante cálculo de métricas."""
    pass


class MetricsEngine:
    """
    Engine para cálculo de métricas pedagógicas.
    
    Implementa as métricas especificadas em docs/METRICS.md:
    - Métricas temporais
    - Métricas de desempenho
    - Métricas comportamentais
    - Métricas de self-assessment
    """
    
    def __init__(self, session_timeout_minutes: int = 30):
        """
        Inicializa engine de métricas.
        
        Args:
            session_timeout_minutes: Timeout para detecção de sessões
        """
        self.session_timeout_minutes = session_timeout_minutes
        self.session_timeout_seconds = session_timeout_minutes * 60
    
    def compute_all_metrics(
        self,
        events: List[BaseEvent],
        sessions: List[Session],
        case_id: str,
        student_id: str,
        task_id: str
    ) -> List[MetricResult]:
        """
        Calcula todas as métricas disponíveis.
        
        Args:
            events: Lista de eventos ordenados por timestamp
            sessions: Lista de sessões detectadas
            case_id: ID do caso
            student_id: ID do estudante (será hasheado)
            task_id: ID da tarefa
        
        Returns:
            Lista de métricas calculadas
        """
        if not events:
            logger.warning("[MetricsEngine.compute_all_metrics] - no_events_for_metrics", case_id=case_id, task_id=task_id)
            return []
        
        student_hash = self._hash_student_id(student_id)
        metrics = []
        
        logger.info(
            "[MetricsEngine.compute_all_metrics] - metrics_computation_started",
            case_id=case_id,
            task_id=task_id,
            events=len(events),
            sessions=len(sessions)
        )
        
        metrics.extend(self._compute_temporal_metrics(
            events, sessions, case_id, student_hash, task_id
        ))
        
        metrics.extend(self._compute_performance_metrics(
            events, case_id, student_hash, task_id
        ))
        
        metrics.extend(self._compute_behavioral_metrics(
            events, case_id, student_hash, task_id
        ))
        
        metrics.extend(self._compute_self_assessment_metrics(
            events, case_id, student_hash, task_id
        ))
        
        logger.info(
            "[MetricsEngine.compute_all_metrics] - metrics_computation_completed",
            case_id=case_id,
            task_id=task_id,
            metrics=len(metrics)
        )
        
        return metrics
    
# Métricas temporais    
    def _compute_temporal_metrics(
        self,
        events: List[BaseEvent],
        sessions: List[Session],
        case_id: str,
        student_hash: str,
        task_id: str
    ) -> List[MetricResult]:
        """Calcula métricas temporais."""
        metrics = []
        
        time_active = self._compute_time_active(events)
        metrics.append(MetricResult(
            id=self._generate_metric_id(case_id, "time_active_seconds"),
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            metric_name="time_active_seconds",
            metric_value=float(time_active)
        ))
        
        time_to_success = self._compute_time_to_first_success(events)
        if time_to_success is not None:
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "time_to_first_success_seconds"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="time_to_first_success_seconds",
                metric_value=float(time_to_success)
            ))
        
        metrics.append(MetricResult(
            id=self._generate_metric_id(case_id, "sessions_count"),
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            metric_name="sessions_count",
            metric_value=float(len(sessions))
        ))
        
        if sessions:
            avg_duration = sum(s.duration_seconds for s in sessions) / len(sessions)
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "avg_session_duration_seconds"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="avg_session_duration_seconds",
                metric_value=float(avg_duration)
            ))
        
        return metrics
    
    def _compute_time_active(self, events: List[BaseEvent]) -> int:
        """
        Calcula tempo ativo total em segundos.
        
        Tempo ativo = soma dos intervalos entre eventos consecutivos,
        limitado ao session timeout.
        """
        time_active = 0
        
        for i in range(len(events) - 1):
            delta = (events[i + 1].timestamp - events[i].timestamp).total_seconds()
            time_active += min(delta, self.session_timeout_seconds)
        
        return int(time_active)
    
    def _compute_time_to_first_success(self, events: List[BaseEvent]) -> Optional[int]:
        """
        Tempo em segundos até primeira execução com rate = 100%.
        
        Returns:
            Segundos até sucesso, ou None se nunca teve sucesso
        """
        first_event = events[0]
        
        for event in events:
            if isinstance(event, ExecEvent) and event.rate == 100:
                return int((event.timestamp - first_event.timestamp).total_seconds())
        
        return None
    
# Métricas de desempenho    
    def _compute_performance_metrics(
        self,
        events: List[BaseEvent],
        case_id: str,
        student_hash: str,
        task_id: str
    ) -> List[MetricResult]:
        """Calcula métricas de desempenho."""
        metrics = []
        
        attempts = self._compute_attempts_to_success(events)
        if attempts is not None:
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "attempts_to_success"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="attempts_to_success",
                metric_value=float(attempts)
            ))
        
        final_rate = self._compute_final_success_rate(events)
        if final_rate is not None:
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "final_success_rate"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="final_success_rate",
                metric_value=float(final_rate)
            ))
        
        trajectory = self._compute_success_trajectory(events)
        if trajectory:
            pattern = self._analyze_trajectory_pattern(trajectory)
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "success_trajectory_pattern"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="success_trajectory_pattern",
                metric_value=float(pattern["improvement_rate"] or 0),
                metadata={"pattern": pattern["pattern"], "trajectory": trajectory}
            ))
        
        return metrics
    
    def _compute_attempts_to_success(self, events: List[BaseEvent]) -> Optional[int]:
        """
        Conta execuções até primeira rate = 100%.
        
        Returns:
            Número de ExecEvents até sucesso, ou None se sem sucesso
        """
        attempt_count = 0
        
        for event in events:
            if isinstance(event, ExecEvent):
                attempt_count += 1
                if event.rate == 100:
                    return attempt_count
        
        return None
    
    def _compute_final_success_rate(self, events: List[BaseEvent]) -> Optional[int]:
        """Retorna rate do último ExecEvent."""
        exec_events = [e for e in events if isinstance(e, ExecEvent)]
        
        if exec_events and exec_events[-1].rate is not None:
            return exec_events[-1].rate
        
        return None
    
    def _compute_success_trajectory(self, events: List[BaseEvent]) -> List[Dict]:
        """Extrai trajectory de success rate ao longo do tempo."""
        trajectory = []
        attempt = 0
        
        for event in events:
            if isinstance(event, ExecEvent) and event.rate is not None:
                attempt += 1
                trajectory.append({
                    "timestamp": event.timestamp.isoformat(),
                    "rate": event.rate,
                    "attempt": attempt
                })
        
        return trajectory
    
    def _analyze_trajectory_pattern(self, trajectory: List[Dict]) -> Dict[str, Any]:
        """Analisa padrão de melhoria na trajectory."""
        if len(trajectory) == 1 and trajectory[0]["rate"] == 100:
            return {"pattern": "instant", "improvement_rate": None}
        
        rates = [t["rate"] for t in trajectory]
        
        if len(rates) >= 5 and len(set(rates[-5:])) == 1:
            return {"pattern": "plateau", "improvement_rate": 0}
        
        deltas = [abs(rates[i + 1] - rates[i]) for i in range(len(rates) - 1)]
        if deltas and max(deltas) > 30:
            return {"pattern": "erratic", "improvement_rate": None}
        
        if all(rates[i] <= rates[i + 1] for i in range(len(rates) - 1)):
            improvement_rate = (rates[-1] - rates[0]) / len(rates) if len(rates) > 1 else 0
            return {"pattern": "steady_improvement", "improvement_rate": improvement_rate}
        
        return {"pattern": "steady_improvement", "improvement_rate": None}
    
# Métricas comportamentais    
    def _compute_behavioral_metrics(
        self,
        events: List[BaseEvent],
        case_id: str,
        student_hash: str,
        task_id: str
    ) -> List[MetricResult]:
        """Calcula métricas comportamentais."""
        metrics = []
        
        edit_exec_ratio = self._compute_edit_exec_ratio(events)
        metrics.append(MetricResult(
            id=self._generate_metric_id(case_id, "edit_exec_ratio"),
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            metric_name="edit_exec_ratio",
            metric_value=edit_exec_ratio
        ))
        
        cramming = self._detect_cramming(events)
        metrics.append(MetricResult(
            id=self._generate_metric_id(case_id, "cramming_detected"),
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            metric_name="cramming_detected",
            metric_value=float(cramming["is_cramming"]),
            metadata=cramming
        ))
        
        trial_error = self._detect_trial_and_error(events)
        metrics.append(MetricResult(
            id=self._generate_metric_id(case_id, "trial_and_error_detected"),
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            metric_name="trial_and_error_detected",
            metric_value=float(trial_error["is_trial_error"]),
            metadata=trial_error
        ))
        
        return metrics
    
    def _compute_edit_exec_ratio(self, events: List[BaseEvent]) -> float:
        """
        Calcula ratio entre eventos de edição e execução.
        
        Returns:
            edit_count / exec_count, ou 0.0 se sem execuções
        """
        edit_count = sum(
            1 for e in events
            if isinstance(e, MoveEvent) and e.action == "EDIT"
        )
        exec_count = sum(1 for e in events if isinstance(e, ExecEvent))
        
        if exec_count == 0:
            return 0.0
        
        return edit_count / exec_count
    
    def _detect_cramming(self, events: List[BaseEvent]) -> Dict[str, Any]:
        """
        Detecta se trabalho foi concentrado (single session).
        
        Critério simplificado: se todos eventos cabem em uma janela
        menor que 2x session_timeout, é cramming.
        """
        if len(events) <= 1:
            return {"is_cramming": False, "confidence": 0.0}
        
        total_duration = (events[-1].timestamp - events[0].timestamp).total_seconds()
        threshold = 2 * self.session_timeout_seconds
        
        is_cramming = total_duration < threshold
        confidence = 1.0 - (total_duration / threshold) if is_cramming else 0.0
        
        return {
            "is_cramming": is_cramming,
            "confidence": round(min(confidence, 1.0), 2),
            "total_duration_seconds": int(total_duration)
        }
    
    def _detect_trial_and_error(self, events: List[BaseEvent]) -> Dict[str, Any]:
        """
        Detecta padrão de trial-and-error.
        
        Critério: múltiplas execuções sem edições entre elas.
        """
        exec_events = [e for e in events if isinstance(e, ExecEvent)]
        
        if len(exec_events) < 3:
            return {"is_trial_error": False, "confidence": 0.0}
        
        # Conta sequências de execuções consecutivas
        consecutive_execs = []
        current_seq = 1
        
        for i in range(len(events) - 1):
            if isinstance(events[i], ExecEvent) and isinstance(events[i + 1], ExecEvent):
                current_seq += 1
            else:
                if current_seq >= 2:
                    consecutive_execs.append(current_seq)
                current_seq = 1
        
        if current_seq >= 2:
            consecutive_execs.append(current_seq)
        
        # Trial-error se há múltiplas sequências de 2+ execuções consecutivas
        is_trial_error = len(consecutive_execs) >= 2
        confidence = min(len(consecutive_execs) / 3, 1.0) if is_trial_error else 0.0
        
        return {
            "is_trial_error": is_trial_error,
            "confidence": round(confidence, 2),
            "consecutive_exec_sequences": consecutive_execs
        }
    

# Métricas de self-assessment    
    def _compute_self_assessment_metrics(
        self,
        events: List[BaseEvent],
        case_id: str,
        student_hash: str,
        task_id: str
    ) -> List[MetricResult]:
        """Calcula métricas de auto-avaliação."""
        metrics = []
        
        self_events = [e for e in events if isinstance(e, SelfEvent)]
        
        if not self_events:
            return metrics
        
        autonomy_scores = [e.autonomy for e in self_events if e.autonomy is not None]
        if autonomy_scores:
            avg_autonomy = sum(autonomy_scores) / len(autonomy_scores)
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "autonomy_score_avg"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="autonomy_score_avg",
                metric_value=float(avg_autonomy)
            ))
        
        help_effectiveness = self._compute_help_effectiveness(events, self_events)
        if help_effectiveness is not None:
            metrics.append(MetricResult(
                id=self._generate_metric_id(case_id, "help_effectiveness"),
                case_id=case_id,
                student_hash=student_hash,
                task_id=task_id,
                metric_name="help_effectiveness",
                metric_value=help_effectiveness
            ))
        
        return metrics
    
    def _compute_help_effectiveness(
        self,
        events: List[BaseEvent],
        self_events: List[SelfEvent]
    ) -> Optional[float]:
        """
        Calcula se ajuda recebida correlaciona com sucesso.
        
        Métrica simplificada: se recebeu ajuda E teve sucesso, effectiveness = 1.0
        """
        if not self_events:
            return None
        
        received_help = any(e.has_any_help() for e in self_events)
        had_success = any(
            isinstance(e, ExecEvent) and e.rate == 100
            for e in events
        )
        
        if not received_help:
            return 0.0
        
        return 1.0 if had_success else 0.5
    
# Auxiliares    
    def _generate_metric_id(self, case_id: str, metric_name: str) -> str:
        """Gera ID determinístico para métrica."""
        data = f"{case_id}|{metric_name}"
        hash_obj = hashlib.sha256(data.encode('utf-8'))
        return hash_obj.hexdigest()[:16]
    
    def _hash_student_id(self, student_id: str) -> str:
        """Anonimiza student_id via SHA256."""
        hash_obj = hashlib.sha256(student_id.encode('utf-8'))
        return hash_obj.hexdigest()[:8]
    
    def save_metrics(self, metrics: List[MetricResult], db_path: str) -> int:
        """
        Persiste métricas no banco SQLite.
        
        Args:
            metrics: Lista de métricas calculadas
            db_path: Caminho do banco SQLite
        
        Returns:
            Número de métricas inseridas
        
        Raises:
            MetricsError: Se houver erro na inserção
        """
        if not metrics:
            logger.info("[MetricsEngine.save_metrics] - no_metrics_to_save")
            return 0
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            rows = [metric.to_db_row() for metric in metrics]
            cursor.executemany(
                """
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id,
                    metric_name, metric_value, metadata, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )
            
            inserted = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(
                "[MetricsEngine.save_metrics] - metrics_saved",
                metrics=len(metrics),
                inserted=inserted
            )
            
            return inserted
        
        except sqlite3.Error as e:
            raise MetricsError(f"Failed to save metrics: {e}")


def get_metrics_from_db(
    db_path: str,
    case_id: str = None,
    metric_name: str = None,
    limit: int = None
) -> List[dict]:
    """
    Recupera métricas do banco SQLite.
    
    Args:
        db_path: Caminho do banco SQLite
        case_id: Filtro opcional por case_id
        metric_name: Filtro opcional por metric_name
        limit: Limite de resultados
    
    Returns:
        Lista de métricas como dicionários
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM metrics WHERE 1=1"
    params = []
    
    if case_id:
        query += " AND case_id = ?"
        params.append(case_id)
    
    if metric_name:
        query += " AND metric_name = ?"
        params.append(metric_name)
    
    query += " ORDER BY computed_at DESC"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]
