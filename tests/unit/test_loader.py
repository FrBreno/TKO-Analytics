"""
Testes para SQLiteLoader.

Testa carregamento de eventos no banco de dados SQLite.
"""

import pytest
import sqlite3
from datetime import datetime, timedelta

from tko_analytics.etl.init_db import init_database
from tko_analytics.etl.loader import SQLiteLoader, LoadError
from tko_analytics.models import ExecEvent, MoveEvent, SelfEvent


@pytest.fixture
def temp_db(tmp_path):
    """Fixture para criar banco de dados temporário."""
    db_path = tmp_path / "test.db"
    init_database(str(db_path))
    return db_path


@pytest.fixture
def loader(temp_db):
    """Fixture para criar loader com banco temporário."""
    return SQLiteLoader(str(temp_db))


class TestSQLiteLoaderInitialization:
    """Testes para inicialização do loader."""
    
    def test_loader_with_valid_db(self, temp_db):
        """Testa criação de loader com banco válido."""
        loader = SQLiteLoader(str(temp_db))
        
        assert loader.db_path == temp_db
        assert loader.batch_size == 1000
        assert loader.events_loaded == 0
        assert loader.events_skipped == 0
    
    def test_loader_with_nonexistent_db(self, tmp_path):
        """Testa erro ao criar loader com banco inexistente."""
        db_path = tmp_path / "nonexistent.db"
        
        with pytest.raises(LoadError) as exc_info:
            SQLiteLoader(str(db_path))
        
        assert "not found" in str(exc_info.value).lower()
    
    def test_loader_custom_batch_size(self, temp_db):
        """Testa loader com batch_size customizado."""
        loader = SQLiteLoader(str(temp_db), batch_size=500)
        
        assert loader.batch_size == 500


class TestLoadEvents:
    """Testes para carregamento de eventos."""
    
    def test_load_single_exec_event(self, loader):
        """Testa carregamento de um único ExecEvent."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=120
            )
        ]
        
        count = loader.load_events(events, student_id="aluno_001")
        
        assert count == 1
        assert loader.events_loaded == 1
        assert loader.get_event_count() == 1
    
    def test_load_multiple_events(self, loader):
        """Testa carregamento de múltiplos eventos."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(timestamp=base_time, task_id="task_001", mode="FULL", rate=80, size=100),
            MoveEvent(timestamp=base_time + timedelta(minutes=1), task_id="task_001", action="PICK"),
            SelfEvent(timestamp=base_time + timedelta(minutes=5), task_id="task_001", rate=90, autonomy=8, study_minutes=60),
        ]
        
        count = loader.load_events(events, student_id="aluno_002")
        
        assert count == 3
        assert loader.get_event_count() == 3
    
    def test_load_empty_list(self, loader):
        """Testa carregamento de lista vazia."""
        count = loader.load_events([], student_id="aluno_003")
        
        assert count == 0
        assert loader.get_event_count() == 0
    
    def test_load_with_custom_case_id(self, loader):
        """Testa carregamento com case_id customizado."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=120
            )
        ]
        
        loader.load_events(events, student_id="aluno_004", case_id="custom_case_123")
        
        # Verifica que evento foi salvo com case_id correto
        loaded_events = loader.get_events(case_id="custom_case_123")
        assert len(loaded_events) == 1
        assert loaded_events[0]["case_id"] == "custom_case_123"
    
    def test_load_with_session_id(self, loader):
        """Testa carregamento com session_id."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=120
            )
        ]
        
        loader.load_events(events, student_id="aluno_005", session_id="session_abc")
        
        loaded_events = loader.get_events()
        assert loaded_events[0]["session_id"] == "session_abc"


class TestEventMapping:
    """Testes para mapeamento de eventos para schema."""
    
    def test_exec_event_metadata(self, loader):
        """Testa que ExecEvent metadata é salvo corretamente."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=120,
                error="COMP"
            )
        ]
        
        loader.load_events(events, student_id="aluno_006")
        
        loaded = loader.get_events()[0]
        import json
        metadata = json.loads(loaded["metadata"])
        
        assert metadata["mode"] == "FULL"
        assert metadata["rate"] == 85
        assert metadata["size"] == 120
        assert metadata["error"] == "COMP"
    
    def test_move_event_activity_name(self, loader):
        """Testa que MoveEvent tem activity correto."""
        events = [
            MoveEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                action="PICK"
            )
        ]
        
        loader.load_events(events, student_id="aluno_007")
        
        loaded = loader.get_events()[0]
        assert loaded["activity"] == "task_navigation"
        assert loaded["event_type"] == "MoveEvent"
    
    def test_self_event_metadata(self, loader):
        """Testa que SelfEvent metadata inclui help sources."""
        events = [
            SelfEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                rate=75,
                autonomy=5,
                help_human="professor",
                help_iagen="gpt",
                study_minutes=120
            )
        ]
        
        loader.load_events(events, student_id="aluno_008")
        
        loaded = loader.get_events()[0]
        import json
        metadata = json.loads(loaded["metadata"])
        
        assert metadata["autonomy"] == 5
        assert metadata["has_help"] is True
        assert "human" in metadata["help_sources"]
        assert metadata["help_sources"]["human"] == "professor"
        assert metadata["study_minutes"] == 120


class TestDuplicateHandling:
    """Testes para tratamento de duplicatas."""
    
    def test_duplicate_events_skipped(self, loader):
        """Testa que eventos duplicados são skipados."""
        event = ExecEvent(
            timestamp=datetime(2024, 1, 15, 10, 0, 0),
            task_id="task_001",
            mode="FULL",
            rate=85,
            size=120
        )
        
        # Carrega primeira vez
        loader.load_events([event], student_id="aluno_009", case_id="case_dup")
        assert loader.events_loaded == 1
        
        # Tenta carregar duplicata
        loader2 = SQLiteLoader(str(loader.db_path))
        loader2.load_events([event], student_id="aluno_009", case_id="case_dup")
        
        # Duplicata deve ser skipada
        assert loader2.events_skipped == 1
        assert loader2.get_event_count(case_id="case_dup") == 1  # Apenas 1 evento


class TestBatchLoading:
    """Testes para carregamento em batch."""
    
    def test_large_batch(self, loader):
        """Testa carregamento de muitos eventos em batches."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Cria 2500 eventos (maior que batch_size padrão de 1000)
        events = [
            ExecEvent(
                timestamp=base_time + timedelta(seconds=i),
                task_id=f"task_{i % 10}",
                mode="FULL",
                rate=80 + (i % 20),
                size=100
            )
            for i in range(2500)
        ]
        
        count = loader.load_events(events, student_id="aluno_010")
        
        assert count == 2500
        assert loader.get_event_count() == 2500
    
    def test_custom_batch_size(self, temp_db):
        """Testa loader com batch_size pequeno."""
        loader = SQLiteLoader(str(temp_db), batch_size=10)
        
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = [
            ExecEvent(
                timestamp=base_time + timedelta(seconds=i),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=100
            )
            for i in range(25)
        ]
        
        count = loader.load_events(events, student_id="aluno_011")
        
        # Deve processar em 3 batches (10 + 10 + 5)
        assert count == 25


class TestStudentAnonymization:
    """Testes para anonimização de student_id."""
    
    def test_student_id_is_hashed(self, loader):
        """Testa que student_id é hasheado no banco."""
        events = [
            ExecEvent(
                timestamp=datetime(2024, 1, 15, 10, 0, 0),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=120
            )
        ]
        
        loader.load_events(events, student_id="aluno_012_sensitive")
        
        loaded = loader.get_events()[0]
        
        # Student hash não deve ser o ID original
        assert loaded["student_hash"] != "aluno_012_sensitive"
        # Deve ser um hash SHA256 (64 caracteres hex)
        assert len(loaded["student_hash"]) == 64
    
    def test_same_student_same_hash(self, loader):
        """Testa que mesmo student_id gera mesmo hash."""
        event1 = ExecEvent(
            timestamp=datetime(2024, 1, 15, 10, 0, 0),
            task_id="task_001",
            mode="FULL",
            rate=85,
            size=100
        )
        event2 = ExecEvent(
            timestamp=datetime(2024, 1, 15, 10, 1, 0),
            task_id="task_002",
            mode="FULL",
            rate=90,
            size=100
        )
        
        loader.load_events([event1], student_id="aluno_013")
        loader.load_events([event2], student_id="aluno_013")
        
        loaded = loader.get_events()
        
        # Ambos eventos devem ter mesmo student_hash
        assert loaded[0]["student_hash"] == loaded[1]["student_hash"]


class TestEventRetrieval:
    """Testes para recuperação de eventos."""
    
    def test_get_events_by_case_id(self, loader):
        """Testa filtro por case_id."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Carrega eventos em 2 casos diferentes
        loader.load_events([
            ExecEvent(timestamp=base_time, task_id="task_001", mode="FULL", rate=80, size=100)
        ], student_id="aluno_014", case_id="case_A")
        
        loader.load_events([
            ExecEvent(timestamp=base_time, task_id="task_002", mode="FULL", rate=90, size=100)
        ], student_id="aluno_015", case_id="case_B")
        
        # Recupera apenas case_A
        events_a = loader.get_events(case_id="case_A")
        
        assert len(events_a) == 1
        assert events_a[0]["case_id"] == "case_A"
    
    def test_get_events_by_task_id(self, loader):
        """Testa filtro por task_id."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        loader.load_events([
            ExecEvent(timestamp=base_time, task_id="task_001", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=1), task_id="task_002", mode="FULL", rate=90, size=100),
        ], student_id="aluno_016")
        
        events = loader.get_events(task_id="task_002")
        
        assert len(events) == 1
        assert events[0]["task_id"] == "task_002"
    
    def test_get_events_limit(self, loader):
        """Testa limit na recuperação."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        events = [
            ExecEvent(
                timestamp=base_time + timedelta(seconds=i),
                task_id="task_001",
                mode="FULL",
                rate=85,
                size=100
            )
            for i in range(50)
        ]
        
        loader.load_events(events, student_id="aluno_017")
        
        # Recupera apenas 10
        loaded = loader.get_events(limit=10)
        
        assert len(loaded) == 10
    
    def test_get_events_ordered_by_timestamp(self, loader):
        """Testa que eventos são retornados em ordem temporal."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Carrega em ordem não-cronológica
        events = [
            ExecEvent(timestamp=base_time + timedelta(minutes=5), task_id="task_003", mode="FULL", rate=100, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=1), task_id="task_001", mode="FULL", rate=80, size=100),
            ExecEvent(timestamp=base_time + timedelta(minutes=3), task_id="task_002", mode="FULL", rate=90, size=100),
        ]
        
        loader.load_events(events, student_id="aluno_018")
        
        loaded = loader.get_events()
        
        # Deve retornar ordenado por timestamp
        assert loaded[0]["task_id"] == "task_001"
        assert loaded[1]["task_id"] == "task_002"
        assert loaded[2]["task_id"] == "task_003"


class TestDatabaseIntegrity:
    """Testes de integridade do banco."""
    
    def test_indexes_created(self, temp_db):
        """Testa que índices foram criados."""
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND tbl_name='events'
        """)
        
        indexes = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Verifica índices principais
        assert "idx_events_case_timestamp" in indexes
        assert "idx_events_student" in indexes
        assert "idx_events_task" in indexes
