"""
Testes para SessionDetector.
"""

import pytest
from datetime import datetime, timedelta

from src.models.events import ExecEvent, MoveEvent, SelfEvent
from src.etl.session_detector import (
    SessionDetector,
    SessionError,
    get_sessions_from_db
)
from src.etl.init_db import init_database


@pytest.fixture
def temp_db(tmp_path):
    """Cria banco temporário para testes."""
    db_path = tmp_path / "test_sessions.db"
    init_database(str(db_path))
    return str(db_path)


@pytest.fixture
def detector():
    """Cria detector com timeout padrão de 30 minutos."""
    return SessionDetector(timeout_minutes=30)


@pytest.fixture
def sample_events():
    """Cria lista de eventos de exemplo para testes."""
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    events = [
        MoveEvent(
            timestamp=base_time,
            task_id="calculadora",
            action="PICK"
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=5),
            task_id="calculadora",
            mode="FULL",
            rate=50,
            size=80
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=10),
            task_id="calculadora",
            mode="FULL",
            rate=100,
            size=90
        ),
        
        # Gap de 40 minutos (nova sessão)
        MoveEvent(
            timestamp=base_time + timedelta(minutes=50),
            task_id="calculadora",
            action="EDIT"
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=55),
            task_id="calculadora",
            mode="FULL",
            rate=100,
            size=95
        ),
        
        # Mudança de tarefa (nova sessão)
        MoveEvent(
            timestamp=base_time + timedelta(minutes=60),
            task_id="animal",
            action="PICK"
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=65),
            task_id="animal",
            mode="FULL",
            rate=100,
            size=50
        ),
        SelfEvent(
            timestamp=base_time + timedelta(minutes=70),
            task_id="animal",
            rate=100,
            autonomy=9
        ),
        MoveEvent(
            timestamp=base_time + timedelta(minutes=75),
            task_id="animal",
            action="DOWN"
        ),
    ]
    
    return events


class TestSessionDetectorInitialization:
    """Testes de inicialização do detector."""
    
    def test_default_timeout(self):
        """Testa timeout padrão de 30 minutos."""
        detector = SessionDetector()
        assert detector.timeout_minutes == 30
        assert detector.timeout_delta == timedelta(minutes=30)
    
    def test_custom_timeout(self):
        """Testa timeout customizado."""
        detector = SessionDetector(timeout_minutes=15)
        assert detector.timeout_minutes == 15
        assert detector.timeout_delta == timedelta(minutes=15)
    
    def test_invalid_timeout(self):
        """Testa erro com timeout inválido."""
        with pytest.raises(ValueError, match="timeout_minutes must be positive"):
            SessionDetector(timeout_minutes=0)
        
        with pytest.raises(ValueError):
            SessionDetector(timeout_minutes=-10)


class TestSessionDetection:
    """Testes de detecção de sessões."""
    
    def test_single_session(self, detector):
        """Testa detecção de sessão única."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=5),
                task_id="task1",
                mode="FULL",
                rate=75,
                size=100
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=10),
                task_id="task1",
                mode="FULL",
                rate=100,
                size=110
            ),
        ]
        
        sessions = detector.detect_sessions(
            events,
            case_id="case1",
            student_id="student1"
        )
        
        assert len(sessions) == 1
        assert sessions[0].task_id == "task1"
        assert sessions[0].event_count == 3
        assert sessions[0].exec_count == 2
        assert sessions[0].move_count == 1
        assert sessions[0].self_count == 0
        assert sessions[0].duration_seconds == 600
    
    def test_multiple_sessions_by_timeout(self, detector, sample_events):
        """Testa múltiplas sessões separadas por timeout."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        
        # Deve detectar 3 sessões
        assert len(sessions) == 3
        
        # Sessão 1: calculadora (3 eventos, 10 min)
        assert sessions[0].task_id == "calculadora"
        assert sessions[0].event_count == 3
        assert sessions[0].duration_seconds == 600
        
        # Sessão 2: calculadora (2 eventos, 5 min)
        assert sessions[1].task_id == "calculadora"
        assert sessions[1].event_count == 2
        assert sessions[1].duration_seconds == 300
        
        # Sessão 3: animal (4 eventos, 15 min)
        assert sessions[2].task_id == "animal"
        assert sessions[2].event_count == 4
        assert sessions[2].duration_seconds == 900
    
    def test_session_by_task_change(self, detector):
        """Testa nova sessão quando tarefa muda."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=5),
                task_id="task1",
                mode="FULL",
                rate=50,
                size=80
            ),
            # Muda tarefa sem gap grande
            MoveEvent(
                timestamp=base_time + timedelta(minutes=6),
                task_id="task2",
                action="PICK"
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=10),
                task_id="task2",
                mode="FULL",
                rate=100,
                size=90
            ),
        ]
        
        sessions = detector.detect_sessions(
            events,
            case_id="case1",
            student_id="student1"
        )
        
        # Deve criar 2 sessões mesmo sem timeout
        assert len(sessions) == 2
        assert sessions[0].task_id == "task1"
        assert sessions[1].task_id == "task2"
    
    def test_empty_events(self, detector):
        """Testa com lista vazia de eventos."""
        sessions = detector.detect_sessions(
            [],
            case_id="case1",
            student_id="student1"
        )
        
        assert sessions == []
    
    def test_single_event(self, detector):
        """Testa com evento único."""
        event = MoveEvent(
            timestamp=datetime(2024, 1, 15, 10, 0, 0),
            task_id="task1",
            action="PICK"
        )
        
        sessions = detector.detect_sessions(
            [event],
            case_id="case1",
            student_id="student1"
        )
        
        assert len(sessions) == 1
        assert sessions[0].event_count == 1
        assert sessions[0].duration_seconds == 0
    
    def test_events_not_sorted(self, detector):
        """Testa erro quando eventos não estão ordenados."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time + timedelta(minutes=10), task_id="task1", action="PICK"),
            MoveEvent(timestamp=base_time, task_id="task1", action="EDIT"),  # Fora de ordem
        ]
        
        with pytest.raises(SessionError, match="Events not sorted"):
            detector.detect_sessions(events, case_id="case1", student_id="student1")


class TestSessionAttributes:
    """Testes de atributos das sessões."""
    
    def test_session_id_deterministic(self, detector):
        """Testa que session_id é determinístico."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=5),
                task_id="task1",
                mode="FULL",
                rate=100,
                size=100
            ),
        ]
        
        sessions1 = detector.detect_sessions(events, "case1", "student1")
        sessions2 = detector.detect_sessions(events, "case1", "student1")
        
        assert sessions1[0].id == sessions2[0].id
    
    def test_student_hash(self, detector):
        """Testa que student_id é hasheado."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
        ]
        
        sessions = detector.detect_sessions(events, "case1", "student_abc_123")
        
        # Hash deve ser 8 caracteres hex
        assert len(sessions[0].student_hash) == 8
        assert sessions[0].student_hash != "student_abc_123"
        assert all(c in "0123456789abcdef" for c in sessions[0].student_hash)
    
    def test_event_type_counts(self, detector):
        """Testa contadores de tipos de eventos."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
            MoveEvent(timestamp=base_time + timedelta(minutes=1), task_id="task1", action="EDIT"),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=2),
                task_id="task1",
                mode="FULL",
                rate=50,
                size=80
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=5),
                task_id="task1",
                mode="FULL",
                rate=100,
                size=90
            ),
            SelfEvent(
                timestamp=base_time + timedelta(minutes=10),
                task_id="task1",
                rate=100,
                autonomy=8
            ),
        ]
        
        sessions = detector.detect_sessions(events, "case1", "student1")
        
        assert len(sessions) == 1
        assert sessions[0].event_count == 5
        assert sessions[0].exec_count == 2
        assert sessions[0].move_count == 2
        assert sessions[0].self_count == 1


class TestSessionPersistence:
    """Testes de persistência de sessões."""
    
    def test_save_sessions(self, detector, sample_events, temp_db):
        """Testa salvamento de sessões no banco."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        
        inserted = detector.save_sessions(sessions, temp_db)
        
        assert inserted == 3
    
    def test_save_empty_sessions(self, detector, temp_db):
        """Testa salvamento de lista vazia."""
        inserted = detector.save_sessions([], temp_db)
        assert inserted == 0
    
    def test_save_duplicate_sessions(self, detector, sample_events, temp_db):
        """Testa que duplicatas são ignoradas (INSERT OR IGNORE)."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        
        # Primeira inserção
        inserted1 = detector.save_sessions(sessions, temp_db)
        assert inserted1 == 3
        
        # Segunda inserção (duplicatas)
        inserted2 = detector.save_sessions(sessions, temp_db)
        assert inserted2 == 0  # Nenhuma inserida


class TestSessionRetrieval:
    """Testes de recuperação de sessões do banco."""
    
    def test_get_all_sessions(self, detector, sample_events, temp_db):
        """Testa recuperação de todas as sessões."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        detector.save_sessions(sessions, temp_db)
        
        retrieved = get_sessions_from_db(temp_db)
        
        assert len(retrieved) == 3
        assert all("id" in s for s in retrieved)
        assert all("task_id" in s for s in retrieved)
    
    def test_get_sessions_by_case(self, detector, sample_events, temp_db):
        """Testa filtro por case_id."""
        # Cria sessões para case1
        sessions1 = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        detector.save_sessions(sessions1, temp_db)
        
        # Cria sessões para case2
        sessions2 = detector.detect_sessions(
            sample_events[:3],
            case_id="case2",
            student_id="student2"
        )
        detector.save_sessions(sessions2, temp_db)
        
        # Recupera apenas case1
        retrieved = get_sessions_from_db(temp_db, case_id="case1")
        
        assert len(retrieved) == 3
        assert all(s["case_id"] == "case1" for s in retrieved)
    
    def test_get_sessions_by_task(self, detector, sample_events, temp_db):
        """Testa filtro por task_id."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        detector.save_sessions(sessions, temp_db)
        
        # Recupera apenas sessões de calculadora
        retrieved = get_sessions_from_db(temp_db, task_id="calculadora")
        
        assert len(retrieved) == 2  # 2 sessões de calculadora
        assert all(s["task_id"] == "calculadora" for s in retrieved)
    
    def test_get_sessions_with_limit(self, detector, sample_events, temp_db):
        """Testa limite de resultados."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        detector.save_sessions(sessions, temp_db)
        
        retrieved = get_sessions_from_db(temp_db, limit=2)
        
        assert len(retrieved) == 2
    
    def test_sessions_ordered_by_timestamp(self, detector, sample_events, temp_db):
        """Testa que sessões são ordenadas por timestamp."""
        sessions = detector.detect_sessions(
            sample_events,
            case_id="case1",
            student_id="student1"
        )
        detector.save_sessions(sessions, temp_db)
        
        retrieved = get_sessions_from_db(temp_db)
        
        # Verifica ordenação
        for i in range(len(retrieved) - 1):
            ts1 = datetime.fromisoformat(retrieved[i]["start_timestamp"])
            ts2 = datetime.fromisoformat(retrieved[i + 1]["start_timestamp"])
            assert ts1 <= ts2


class TestSessionDurationCalculation:
    """Testes de cálculo de duração."""
    
    def test_zero_duration_single_event(self, detector):
        """Testa duração zero com evento único."""
        event = MoveEvent(
            timestamp=datetime(2024, 1, 15, 10, 0, 0),
            task_id="task1",
            action="PICK"
        )
        
        sessions = detector.detect_sessions([event], "case1", "student1")
        
        assert sessions[0].duration_seconds == 0
    
    def test_duration_multiple_events(self, detector):
        """Testa cálculo de duração com múltiplos eventos."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=7),
                task_id="task1",
                mode="FULL",
                rate=100,
                size=100
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=15),
                task_id="task1",
                mode="FULL",
                rate=100,
                size=110
            ),
        ]
        
        sessions = detector.detect_sessions(events, "case1", "student1")
        assert sessions[0].duration_seconds == 900
