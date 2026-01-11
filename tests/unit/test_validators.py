"""
Testes para EventValidator.

Testa validações de integridade e consistência de eventos TKO.
"""

import pytest
from datetime import datetime, timedelta

from src.etl import EventValidator, ValidationError, ValidationReport
from src.models import ExecEvent, MoveEvent, SelfEvent


class TestValidationReport:
    """Testes para ValidationReport."""
    
    def test_empty_report(self):
        """Testa relatório vazio."""
        report = ValidationReport()
        
        assert report.total_events == 0
        assert report.valid_events == 0
        assert report.is_valid is True
        assert report.error_rate == 0.0
        assert "VALID" in report.summary()
    
    def test_report_with_errors(self):
        """Testa relatório com erros."""
        report = ValidationReport(total_events=10, valid_events=8)
        report.errors.append(ValidationError(
            event_index=3,
            error_type="DUPLICATE",
            message="Duplicate event"
        ))
        report.errors.append(ValidationError(
            event_index=7,
            error_type="TIMESTAMP_ORDER",
            message="Timestamp backwards"
        ))
        
        assert report.is_valid is False
        assert report.error_rate == 0.2  # 2/10
        assert "INVALID" in report.summary()
        assert "2 errors" in report.summary()
    
    def test_report_with_warnings_only(self):
        """Testa que warnings não tornam relatório inválido."""
        report = ValidationReport(total_events=5, valid_events=5)
        report.warnings.append(ValidationError(
            event_index=2,
            error_type="VALUE_WARNING",
            message="Low rate"
        ))
        
        assert report.is_valid is True
        assert len(report.warnings) == 1
        assert len(report.errors) == 0


class TestTimestampValidation:
    """Testes para validação de timestamps."""
    
    def test_monotonic_timestamps_valid(self):
        """Testa timestamps em ordem crescente (válido)."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=1), task_id="task1", mode="FULL", rate=90, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=2), task_id="task1", mode="FULL", rate=100, size=100),
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert report.valid_events == 3
        assert len(report.errors) == 0
    
    def test_backwards_timestamp_error(self):
        """Testa timestamp retroativo (erro por padrão)."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=5), task_id="task1", mode="FULL", rate=90, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=2), task_id="task1", mode="FULL", rate=100, size=100),  # Volta no tempo
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is False
        assert len(report.errors) == 1
        assert report.errors[0].error_type == "TIMESTAMP_ORDER"
        assert report.errors[0].event_index == 2
    
    def test_backwards_timestamp_warning_mode(self):
        """Testa que allow_backwards_time gera warning em vez de erro."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=5), task_id="task1", mode="FULL", rate=90, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=2), task_id="task1", mode="FULL", rate=100, size=100),
        ]
        
        validator = EventValidator(allow_backwards_time=True)
        report = validator.validate(events)
        
        assert report.is_valid is True  # Sem erros bloqueantes
        assert len(report.warnings) == 1
        assert report.warnings[0].error_type == "TIMESTAMP_ORDER"
    
    def test_same_timestamp_allowed(self):
        """Testa que timestamps iguais são permitidos (não retroativos)."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            MoveEvent(timestamp=base_time, task_id="task2", action="PICK"),  # Mesmo timestamp, task diferente
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert len(report.errors) == 0


class TestDuplicateValidation:
    """Testes para detecção de duplicatas."""
    
    def test_no_duplicates(self):
        """Testa eventos únicos (sem duplicatas)."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=1), task_id="task1", mode="FULL", rate=90, size=100),
            MoveEvent(timestamp=base_time + timedelta(minutes=2), task_id="task2", action="PICK"),  # Timestamp posterior
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert len(report.errors) == 0
    
    def test_exact_duplicate_detected(self):
        """Testa detecção de duplicata exata."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=90, size=100),  # Duplicata
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is False
        assert len(report.errors) == 1
        assert report.errors[0].error_type == "DUPLICATE"
        assert report.errors[0].event_index == 1
    
    def test_different_task_not_duplicate(self):
        """Testa que mesmo timestamp + tipo diferente de task não é duplicata."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time, task_id="task2", mode="FULL", rate=80, size=100),  # Task diferente
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert len(report.errors) == 0
    
    def test_different_event_type_not_duplicate(self):
        """Testa que mesmo timestamp + task mas tipo diferente não é duplicata."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),  # Tipo diferente
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert len(report.errors) == 0


class TestValueRangeValidation:
    """Testes para validação de ranges de valores."""
    
    def test_exec_event_valid_ranges(self):
        """Testa ExecEvent com valores válidos."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task1",
                mode="FULL",
                rate=85,
                size=120
            )
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert len(report.errors) == 0
    
    def test_exec_event_missing_rate_for_full(self):
        """Testa que ExecEvent FULL sem rate é erro (validação adicional)."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task1",
                mode="FREE",  # FREE permite rate=None
                rate=None,
                size=100
            )
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
    
    def test_exec_event_negative_size(self):
        """Testa que Pydantic rejeita size <= 0."""
        from pydantic import ValidationError as PydanticValidationError
        
        with pytest.raises(PydanticValidationError) as exc_info:
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task1",
                mode="FULL",
                rate=80,
                size=0  # Zero não deveria passar (gt=0)
            )
        
        assert "size" in str(exc_info.value).lower()
    
    def test_self_event_low_rate_warning(self):
        """Testa que rate baixo em SelfEvent gera warning."""
        events = [
            SelfEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task1",
                rate=30,  # Rate < 50%
                autonomy=8,
                study_minutes=60
            )
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True  # Warning não bloqueia
        assert len(report.warnings) >= 1
        assert any(w.error_type == "VALUE_WARNING" for w in report.warnings)
    
    def test_self_event_low_autonomy_no_help_warning(self):
        """Testa warning para baixa autonomia sem ajuda reportada."""
        events = [
            SelfEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task1",
                rate=80,
                autonomy=2,  # Autonomia muito baixa
                study_minutes=60
            )
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.is_valid is True
        assert len(report.warnings) >= 1
        assert any(w.error_type == "CONSISTENCY_WARNING" for w in report.warnings)


class TestValidatorConfiguration:
    """Testes para configuração do validador."""
    
    def test_disable_timestamp_check(self):
        """Testa desabilitar validação de timestamps."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time + timedelta(minutes=5), task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=90, size=100),  # Volta no tempo
        ]
        
        validator = EventValidator(check_timestamps=False)
        report = validator.validate(events)
        
        # Não deve detectar erro de timestamp
        assert all(e.error_type != "TIMESTAMP_ORDER" for e in report.errors)
    
    def test_disable_duplicate_check(self):
        """Testa desabilitar detecção de duplicatas."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),  # Duplicata
        ]
        
        validator = EventValidator(check_duplicates=False)
        report = validator.validate(events)
        
        # Não deve detectar duplicata
        assert all(e.error_type != "DUPLICATE" for e in report.errors)
    
    def test_disable_value_range_check(self):
        """Testa desabilitar validação de ranges."""
        events = [
            SelfEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task1",
                rate=30,  # Baixo, geraria warning
                autonomy=2,
                study_minutes=60
            )
        ]
        
        validator = EventValidator(check_value_ranges=False)
        report = validator.validate(events)
        
        # Não deve gerar warnings de valor
        assert all(w.error_type not in ["VALUE_WARNING", "CONSISTENCY_WARNING"] for w in report.warnings)


class TestValidationWithMixedEvents:
    """Testes com múltiplos tipos de eventos."""
    
    def test_complex_validation_scenario(self):
        """Testa cenário complexo com múltiplos erros e warnings."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=1), task_id="task1", mode="FULL", rate=90, size=100),
            ExecEvent(timestamp=base_time, task_id="task1", mode="FULL", rate=80, size=100),  # Duplicata
            MoveEvent(timestamp=base_time - timedelta(minutes=1), task_id="task2", action="PICK"),  # Timestamp retroativo
            SelfEvent(timestamp=base_time + timedelta(minutes=10), task_id="task1", rate=30, autonomy=2, study_minutes=60),  # Warnings
        ]
        
        validator = EventValidator()
        report = validator.validate(events)
        
        assert report.total_events == 5
        assert not report.is_valid
        assert len(report.errors) >= 2
        assert len(report.warnings) >= 1
    
    def test_empty_event_list(self):
        """Testa validação de lista vazia."""
        validator = EventValidator()
        report = validator.validate([])
        
        assert report.total_events == 0
        assert report.is_valid is True
        assert len(report.errors) == 0
