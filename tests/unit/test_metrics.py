"""
Testes para MetricsEngine.
"""

import pytest
from datetime import datetime, timedelta

from tko_analytics.models.events import ExecEvent, MoveEvent, SelfEvent
from tko_analytics.etl.session_detector import SessionDetector
from tko_analytics.metrics import MetricsEngine, get_metrics_from_db
from tko_analytics.etl.init_db import init_database


@pytest.fixture
def temp_db(tmp_path):
    """Cria banco temporário para testes."""
    db_path = tmp_path / "test_metrics.db"
    init_database(str(db_path))
    return str(db_path)


@pytest.fixture
def engine():
    """Cria engine com configuração padrão."""
    return MetricsEngine(session_timeout_minutes=30)


@pytest.fixture
def sample_events():
    """Cria lista de eventos para testes."""
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    return [
        MoveEvent(timestamp=base_time, task_id="calc", action="PICK"),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=5),
            task_id="calc",
            mode="FULL",
            rate=30,
            size=50
        ),
        MoveEvent(
            timestamp=base_time + timedelta(minutes=10),
            task_id="calc",
            action="EDIT"
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=15),
            task_id="calc",
            mode="FULL",
            rate=60,
            size=75
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=20),
            task_id="calc",
            mode="FULL",
            rate=100,
            size=90
        ),
        SelfEvent(
            timestamp=base_time + timedelta(minutes=25),
            task_id="calc",
            rate=100,
            autonomy=8,
            help_human="professor"
        ),
    ]


@pytest.fixture
def sample_sessions(sample_events):
    """Cria sessões a partir dos eventos."""
    detector = SessionDetector(timeout_minutes=30)
    return detector.detect_sessions(
        sample_events,
        case_id="case1",
        student_id="student1"
    )


class TestMetricsEngineInitialization:
    """Testes de inicialização."""
    
    def test_default_timeout(self):
        """Testa timeout padrão."""
        engine = MetricsEngine()
        assert engine.session_timeout_minutes == 30
        assert engine.session_timeout_seconds == 1800
    
    def test_custom_timeout(self):
        """Testa timeout customizado."""
        engine = MetricsEngine(session_timeout_minutes=15)
        assert engine.session_timeout_minutes == 15
        assert engine.session_timeout_seconds == 900


class TestTemporalMetrics:
    """Testes de métricas temporais."""
    
    def test_time_active(self, engine, sample_events):
        """Testa cálculo de time_active."""
        time_active = engine._compute_time_active(sample_events)
        
        # 5 intervalos de 5 minutos = 25 minutos = 1500 segundos
        assert time_active == 1500
    
    def test_time_to_first_success(self, engine, sample_events):
        """Testa tempo até primeiro sucesso."""
        time_to_success = engine._compute_time_to_first_success(sample_events)
        
        # Sucesso no terceiro ExecEvent (20 minutos)
        assert time_to_success == 1200
    
    def test_time_to_first_success_no_success(self, engine):
        """Testa quando nunca teve sucesso."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            ExecEvent(
                timestamp=base_time,
                task_id="calc",
                mode="FULL",
                rate=50,
                size=80
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=5),
                task_id="calc",
                mode="FULL",
                rate=75,
                size=90
            ),
        ]
        
        result = engine._compute_time_to_first_success(events)
        assert result is None
    
    def test_temporal_metrics_all(self, engine, sample_events, sample_sessions):
        """Testa cálculo de todas métricas temporais."""
        metrics = engine._compute_temporal_metrics(
            sample_events,
            sample_sessions,
            "case1",
            "abc12345",
            "calc"
        )
        
        # Deve ter 4 métricas temporais
        assert len(metrics) == 4
        
        metric_names = {m.metric_name for m in metrics}
        assert "time_active_seconds" in metric_names
        assert "time_to_first_success_seconds" in metric_names
        assert "sessions_count" in metric_names
        assert "avg_session_duration_seconds" in metric_names


class TestPerformanceMetrics:
    """Testes de métricas de desempenho."""
    
    def test_attempts_to_success(self, engine, sample_events):
        """Testa contagem de tentativas até sucesso."""
        attempts = engine._compute_attempts_to_success(sample_events)
        
        # 3 ExecEvents, sucesso no terceiro
        assert attempts == 3
    
    def test_attempts_to_success_no_success(self, engine):
        """Testa quando nunca teve sucesso."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            ExecEvent(
                timestamp=base_time,
                task_id="calc",
                mode="FULL",
                rate=50,
                size=80
            ),
        ]
        
        result = engine._compute_attempts_to_success(events)
        assert result is None
    
    def test_final_success_rate(self, engine, sample_events):
        """Testa taxa de sucesso final."""
        final_rate = engine._compute_final_success_rate(sample_events)
        
        # Último ExecEvent tem rate=100
        assert final_rate == 100
    
    def test_success_trajectory(self, engine, sample_events):
        """Testa extração de trajectory."""
        trajectory = engine._compute_success_trajectory(sample_events)
        
        # 3 ExecEvents com rates: 30, 60, 100
        assert len(trajectory) == 3
        assert trajectory[0]["rate"] == 30
        assert trajectory[1]["rate"] == 60
        assert trajectory[2]["rate"] == 100
        assert trajectory[0]["attempt"] == 1
        assert trajectory[2]["attempt"] == 3
    
    def test_trajectory_pattern_steady_improvement(self, engine):
        """Testa detecção de padrão steady_improvement."""
        trajectory = [
            {"timestamp": "2024-01-15T10:00:00", "rate": 25, "attempt": 1},
            {"timestamp": "2024-01-15T10:10:00", "rate": 50, "attempt": 2},
            {"timestamp": "2024-01-15T10:20:00", "rate": 75, "attempt": 3},
            {"timestamp": "2024-01-15T10:30:00", "rate": 100, "attempt": 4},
        ]
        
        pattern = engine._analyze_trajectory_pattern(trajectory)
        
        assert pattern["pattern"] == "steady_improvement"
        assert pattern["improvement_rate"] > 0
    
    def test_trajectory_pattern_instant(self, engine):
        """Testa detecção de sucesso instantâneo."""
        trajectory = [
            {"timestamp": "2024-01-15T10:00:00", "rate": 100, "attempt": 1},
        ]
        
        pattern = engine._analyze_trajectory_pattern(trajectory)
        
        assert pattern["pattern"] == "instant"
        assert pattern["improvement_rate"] is None
    
    def test_trajectory_pattern_plateau(self, engine):
        """Testa detecção de plateau."""
        # Plateau com progresso inicial mas depois estagnação
        trajectory = [
            {"timestamp": "2024-01-15T10:00:00", "rate": 30, "attempt": 1},
            {"timestamp": "2024-01-15T10:10:00", "rate": 50, "attempt": 2},
            {"timestamp": "2024-01-15T10:20:00", "rate": 50, "attempt": 3},
            {"timestamp": "2024-01-15T10:30:00", "rate": 50, "attempt": 4},
            {"timestamp": "2024-01-15T10:40:00", "rate": 50, "attempt": 5},
            {"timestamp": "2024-01-15T10:50:00", "rate": 50, "attempt": 6},
        ]
        
        pattern = engine._analyze_trajectory_pattern(trajectory)
        
        assert pattern["pattern"] == "plateau"
        assert pattern["improvement_rate"] == 0
    
    def test_trajectory_pattern_erratic(self, engine):
        """Testa detecção de padrão errático."""
        trajectory = [
            {"timestamp": "2024-01-15T10:00:00", "rate": 25, "attempt": 1},
            {"timestamp": "2024-01-15T10:10:00", "rate": 75, "attempt": 2},
            {"timestamp": "2024-01-15T10:20:00", "rate": 30, "attempt": 3},
            {"timestamp": "2024-01-15T10:30:00", "rate": 85, "attempt": 4},
        ]
        
        pattern = engine._analyze_trajectory_pattern(trajectory)
        
        assert pattern["pattern"] == "erratic"


class TestBehavioralMetrics:
    """Testes de métricas comportamentais."""
    
    def test_edit_exec_ratio(self, engine, sample_events):
        """Testa cálculo de edit-to-exec ratio."""
        ratio = engine._compute_edit_exec_ratio(sample_events)
        
        # 1 EDIT, 3 EXEC → 1/3 = 0.333...
        assert abs(ratio - 0.333) < 0.01
    
    def test_edit_exec_ratio_no_execs(self, engine):
        """Testa ratio quando não há execuções."""
        events = [
            MoveEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="calc",
                action="EDIT"
            ),
        ]
        
        ratio = engine._compute_edit_exec_ratio(events)
        assert ratio == 0.0
    
    def test_cramming_detection(self, engine, sample_events):
        """Testa detecção de cramming."""
        cramming = engine._detect_cramming(sample_events)
        
        # 25 minutos < 60 minutos (2x timeout) → cramming
        assert cramming["is_cramming"] is True
        assert cramming["confidence"] > 0
    
    def test_cramming_detection_distributed(self, engine):
        """Testa que trabalho distribuído não é cramming."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            MoveEvent(timestamp=base_time, task_id="calc", action="PICK"),
            ExecEvent(
                timestamp=base_time + timedelta(hours=2),
                task_id="calc",
                mode="FULL",
                rate=100,
                size=80
            ),
        ]
        
        cramming = engine._detect_cramming(events)
        assert cramming["is_cramming"] is False
    
    def test_trial_and_error_detection(self, engine):
        """Testa detecção de trial-and-error."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Padrão trial-error: exec-exec-exec sem edições
        events = [
            ExecEvent(
                timestamp=base_time,
                task_id="calc",
                mode="FULL",
                rate=30,
                size=80
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=2),
                task_id="calc",
                mode="FULL",
                rate=30,
                size=80
            ),
            MoveEvent(
                timestamp=base_time + timedelta(minutes=4),
                task_id="calc",
                action="EDIT"
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=6),
                task_id="calc",
                mode="FULL",
                rate=50,
                size=85
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=8),
                task_id="calc",
                mode="FULL",
                rate=50,
                size=85
            ),
        ]
        
        trial_error = engine._detect_trial_and_error(events)
        
        # Tem 2 sequências de execuções consecutivas
        assert trial_error["is_trial_error"] is True
        assert len(trial_error["consecutive_exec_sequences"]) >= 2


class TestSelfAssessmentMetrics:
    """Testes de métricas de auto-avaliação."""
    
    def test_autonomy_score_avg(self, engine, sample_events):
        """Testa cálculo de autonomy score médio."""
        metrics = engine._compute_self_assessment_metrics(
            sample_events,
            "case1",
            "abc12345",
            "calc"
        )
        
        # Tem 1 SelfEvent com autonomy=8
        autonomy_metric = next(
            (m for m in metrics if m.metric_name == "autonomy_score_avg"),
            None
        )
        
        assert autonomy_metric is not None
        assert autonomy_metric.metric_value == 8.0
    
    def test_help_effectiveness_with_success(self, engine, sample_events):
        """Testa help_effectiveness quando teve sucesso."""
        effectiveness = engine._compute_help_effectiveness(
            sample_events,
            [e for e in sample_events if isinstance(e, SelfEvent)]
        )
        
        # Recebeu ajuda E teve sucesso → 1.0
        assert effectiveness == 1.0
    
    def test_help_effectiveness_no_help(self, engine):
        """Testa effectiveness quando não recebeu ajuda."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            SelfEvent(
                timestamp=base_time,
                task_id="calc",
                rate=100,
                autonomy=10
            ),
        ]
        
        self_events = [e for e in events if isinstance(e, SelfEvent)]
        effectiveness = engine._compute_help_effectiveness(events, self_events)
        
        assert effectiveness == 0.0


class TestMetricsIntegration:
    """Testes de integração completa."""
    
    def test_compute_all_metrics(self, engine, sample_events, sample_sessions):
        """Testa cálculo de todas métricas."""
        metrics = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        
        # Deve ter várias métricas
        assert len(metrics) > 10
        
        # Verifica que todas têm atributos obrigatórios
        for metric in metrics:
            assert metric.id
            assert metric.case_id == "case1"
            assert metric.student_hash
            assert metric.task_id == "calc"
            assert metric.metric_name
            assert isinstance(metric.metric_value, (int, float))
    
    def test_empty_events(self, engine):
        """Testa com lista vazia de eventos."""
        metrics = engine.compute_all_metrics(
            events=[],
            sessions=[],
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        
        assert metrics == []


class TestMetricsPersistence:
    """Testes de persistência de métricas."""
    
    def test_save_metrics(self, engine, sample_events, sample_sessions, temp_db):
        """Testa salvamento de métricas no banco."""
        metrics = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        
        inserted = engine.save_metrics(metrics, temp_db)
        
        assert inserted > 0
        assert inserted == len(metrics)
    
    def test_save_empty_metrics(self, engine, temp_db):
        """Testa salvamento de lista vazia."""
        inserted = engine.save_metrics([], temp_db)
        assert inserted == 0
    
    def test_save_metrics_replace(self, engine, sample_events, sample_sessions, temp_db):
        """Testa que métricas duplicadas são substituídas (REPLACE)."""
        metrics = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        
        # Primeira inserção
        inserted1 = engine.save_metrics(metrics, temp_db)
        
        # Modifica valor de uma métrica
        metrics[0].metric_value = 999.0
        
        # Segunda inserção (deve fazer REPLACE)
        inserted2 = engine.save_metrics(metrics, temp_db)
        
        # Verifica que valor foi atualizado
        retrieved = get_metrics_from_db(
            temp_db,
            case_id="case1",
            metric_name=metrics[0].metric_name
        )
        
        assert len(retrieved) == 1
        assert retrieved[0]["metric_value"] == 999.0


class TestMetricsRetrieval:
    """Testes de recuperação de métricas."""
    
    def test_get_all_metrics(self, engine, sample_events, sample_sessions, temp_db):
        """Testa recuperação de todas métricas."""
        metrics = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        engine.save_metrics(metrics, temp_db)
        
        retrieved = get_metrics_from_db(temp_db)
        
        assert len(retrieved) == len(metrics)
    
    def test_get_metrics_by_case(self, engine, sample_events, sample_sessions, temp_db):
        """Testa filtro por case_id."""
        # Case1
        metrics1 = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        engine.save_metrics(metrics1, temp_db)
        
        # Case2
        metrics2 = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case2",
            student_id="student2",
            task_id="calc"
        )
        engine.save_metrics(metrics2, temp_db)
        
        # Recupera apenas case1
        retrieved = get_metrics_from_db(temp_db, case_id="case1")
        
        assert len(retrieved) == len(metrics1)
        assert all(m["case_id"] == "case1" for m in retrieved)
    
    def test_get_metrics_by_name(self, engine, sample_events, sample_sessions, temp_db):
        """Testa filtro por metric_name."""
        metrics = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        engine.save_metrics(metrics, temp_db)
        
        # Recupera apenas time_active
        retrieved = get_metrics_from_db(
            temp_db,
            metric_name="time_active_seconds"
        )
        
        assert len(retrieved) == 1
        assert retrieved[0]["metric_name"] == "time_active_seconds"
    
    def test_get_metrics_with_limit(self, engine, sample_events, sample_sessions, temp_db):
        """Testa limite de resultados."""
        metrics = engine.compute_all_metrics(
            events=sample_events,
            sessions=sample_sessions,
            case_id="case1",
            student_id="student1",
            task_id="calc"
        )
        engine.save_metrics(metrics, temp_db)
        
        retrieved = get_metrics_from_db(temp_db, limit=5)
        
        assert len(retrieved) == 5
