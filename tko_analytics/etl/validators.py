"""
Validadores de eventos TKO.

Este módulo implementa validações de integridade e consistência
para eventos de telemetria TKO antes do carregamento no banco de dados.
"""

import structlog
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Set, Dict, Any, Optional

from tko_analytics.models import BaseEvent, ExecEvent, SelfEvent

logger = structlog.get_logger()


@dataclass
class ValidationError:
    """
    Representa um erro de validação.
    
    Attributes:
        event_index: Índice do evento na lista (0-based)
        error_type: Tipo do erro (TIMESTAMP, DUPLICATE, VALUE_RANGE, etc.)
        message: Mensagem descritiva do erro
        event_data: Dados do evento que causou o erro
    """
    event_index: int
    error_type: str
    message: str
    event_data: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        return f"[{self.error_type}] Event #{self.event_index}: {self.message}"


@dataclass
class ValidationReport:
    """
    Relatório de validação de eventos.
    
    Attributes:
        total_events: Total de eventos validados
        valid_events: Número de eventos válidos
        errors: Lista de erros encontrados
        warnings: Lista de avisos (não bloqueiam carregamento)
    """
    total_events: int = 0
    valid_events: int = 0
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    
    @property
    def is_valid(self) -> bool:
        """Retorna True se não há erros bloqueantes."""
        return len(self.errors) == 0
    
    @property
    def error_rate(self) -> float:
        """Taxa de erros (0.0 a 1.0)."""
        if self.total_events == 0:
            return 0.0
        return len(self.errors) / self.total_events
    
    def summary(self) -> str:
        """Retorna resumo textual da validação."""
        status = "VALID" if self.is_valid else "INVALID"
        return (
            f"{status} - {self.valid_events}/{self.total_events} events valid "
            f"({len(self.errors)} errors, {len(self.warnings)} warnings)"
        )


class EventValidator:
    """
    Validador de eventos TKO.
    
    Realiza validações de integridade e consistência em listas de eventos,
    incluindo checagem de timestamps, duplicatas e ranges de valores.
    """
    
    def __init__(
        self,
        check_timestamps: bool = True,
        check_duplicates: bool = True,
        check_value_ranges: bool = True,
        allow_backwards_time: bool = False
    ):
        """
        Inicializa o validador.
        
        Args:
            check_timestamps: Valida ordem temporal dos eventos
            check_duplicates: Detecta eventos duplicados
            check_value_ranges: Valida ranges de valores (rate, autonomy, etc.)
            allow_backwards_time: Permite timestamps não-monotônicos
        """
        self.check_timestamps = check_timestamps
        self.check_duplicates = check_duplicates
        self.check_value_ranges = check_value_ranges
        self.allow_backwards_time = allow_backwards_time
        
    def validate(self, events: List[BaseEvent]) -> ValidationReport:
        """
        Valida lista de eventos.
        
        Args:
            events: Lista de eventos a validar
            
        Returns:
            Relatório de validação com erros e warnings
        """
        report = ValidationReport(total_events=len(events))
        
        if not events:
            logger.warning("[EventValidator.validate] - validation_empty", message="No events to validate")
            return report
        
        logger.info("[EventValidator.validate] - validation_started", events=len(events))
        
        # Validações
        if self.check_timestamps:
            self._validate_timestamps(events, report)
        
        if self.check_duplicates:
            self._validate_duplicates(events, report)
        
        if self.check_value_ranges:
            self._validate_value_ranges(events, report)
        
        # Contabiliza eventos válidos
        invalid_indices = {err.event_index for err in report.errors}
        report.valid_events = len(events) - len(invalid_indices)
        
        logger.info("[EventValidator.validate] - validation_completed", 
                   valid=report.valid_events,
                   errors=len(report.errors),
                   warnings=len(report.warnings))
        
        return report
    
    def _validate_timestamps(
        self, 
        events: List[BaseEvent], 
        report: ValidationReport
    ) -> None:
        """
        Valida ordem temporal dos eventos.
        
        Verifica se timestamps estão em ordem cronológica (monotônico crescente).
        Se allow_backwards_time=False, timestamps não-monotônicos são erros.
        """
        prev_timestamp: Optional[datetime] = None
        
        for idx, event in enumerate(events):
            if prev_timestamp is not None:
                if event.timestamp < prev_timestamp:
                    error = ValidationError(
                        event_index=idx,
                        error_type="TIMESTAMP_ORDER",
                        message=(
                            f"Timestamp goes backwards: "
                            f"{event.timestamp} < {prev_timestamp}"
                        ),
                        event_data={"timestamp": event.timestamp.isoformat()}
                    )
                    
                    if self.allow_backwards_time:
                        report.warnings.append(error)
                    else:
                        report.errors.append(error)
            
            prev_timestamp = event.timestamp
    
    def _validate_duplicates(
        self, 
        events: List[BaseEvent], 
        report: ValidationReport
    ) -> None:
        """
        Detecta eventos duplicados.
        
        Considera duplicado: mesmo timestamp + task_id + tipo de evento.
        """
        seen: Set[tuple] = set()
        
        for idx, event in enumerate(events):
            # Chave: (timestamp, task_id, tipo)
            key = (
                event.timestamp.isoformat(),
                event.task_id,
                type(event).__name__
            )
            
            if key in seen:
                report.errors.append(ValidationError(
                    event_index=idx,
                    error_type="DUPLICATE",
                    message=(
                        f"Duplicate event: {type(event).__name__} "
                        f"for task '{event.task_id}' at {event.timestamp}"
                    ),
                    event_data={
                        "timestamp": event.timestamp.isoformat(),
                        "task_id": event.task_id,
                        "event_type": type(event).__name__
                    }
                ))
            else:
                seen.add(key)
    
    def _validate_value_ranges(
        self, 
        events: List[BaseEvent], 
        report: ValidationReport
    ) -> None:
        """
        Valida ranges de valores específicos de cada tipo de evento.
        
        - ExecEvent: rate (0-100), size (>0)
        - SelfEvent: rate (0-100), autonomy (0-10), study_minutes (>=0)
        """
        for idx, event in enumerate(events):
            if isinstance(event, ExecEvent):
                self._validate_exec_event(idx, event, report)
            elif isinstance(event, SelfEvent):
                self._validate_self_event(idx, event, report)
    
    def _validate_exec_event(
        self, 
        idx: int, 
        event: ExecEvent, 
        report: ValidationReport
    ) -> None:
        """Valida valores específicos de ExecEvent."""
        if event.mode in ['FULL', 'LOCK'] and event.rate is None:
            report.errors.append(ValidationError(
                event_index=idx,
                error_type="VALUE_MISSING",
                message=f"ExecEvent mode '{event.mode}' requires rate, got None",
                event_data={"mode": event.mode, "rate": None}
            ))
        
        if event.size is not None and event.size <= 0:
            report.errors.append(ValidationError(
                event_index=idx,
                error_type="VALUE_RANGE",
                message=f"ExecEvent size must be > 0, got {event.size}",
                event_data={"size": event.size}
            ))
    
    def _validate_self_event(
        self, 
        idx: int, 
        event: SelfEvent, 
        report: ValidationReport
    ) -> None:
        """Valida valores específicos de SelfEvent."""
        if event.rate is not None and event.rate < 50:
            report.warnings.append(ValidationError(
                event_index=idx,
                error_type="VALUE_WARNING",
                message=f"SelfEvent has low rate: {event.rate}% (< 50%)",
                event_data={"rate": event.rate}
            ))

        if event.autonomy is not None and event.autonomy < 3:
            if not event.has_any_help():
                report.warnings.append(ValidationError(
                    event_index=idx,
                    error_type="CONSISTENCY_WARNING",
                    message=(
                        f"Low autonomy ({event.autonomy}/10) but no help reported. "
                        "This may indicate incomplete self-assessment."
                    ),
                    event_data={"autonomy": event.autonomy, "has_help": False}
                ))
