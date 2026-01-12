"""
Testes de integração para TKOTransformer.

Testa a conversão do formato TKO para CSV do TKO-Analytics.
"""

import csv
from pathlib import Path
from datetime import datetime
from src.tko_integration.parser import TKOLogEvent
from src.tko_integration.scanner import StudentRepo
from src.tko_integration.scanner import ClassroomScanner
from src.tko_integration.transformer import TKOTransformer


def test_pseudonymize_student_id():
    """Testa se a pseudonimização cria hashes consistentes."""
    transformer = TKOTransformer("test-salt")
    hash1 = transformer.pseudonymize_student_id("student1")
    hash2 = transformer.pseudonymize_student_id("student1")
    hash3 = transformer.pseudonymize_student_id("student2")
    
    assert hash1 == hash2
    assert hash1 != hash3
    assert len(hash1) == 8
    assert hash1.isalnum()


def test_normalize_task_key():
    """Testa a remoção de prefixos de tarefas."""
    transformer = TKOTransformer("test-salt")
    
    assert transformer.normalize_task_key("poo@toalha") == "toalha"
    assert transformer.normalize_task_key("fup@exercicio") == "exercicio"
    assert transformer.normalize_task_key("toalha") == "toalha"


def test_event_to_csv_row():
    """Testa a conversão de evento para linha CSV."""
    transformer = TKOTransformer("test-salt")
    event = TKOLogEvent(
        timestamp=datetime(2025, 1, 10, 10, 0, 0),
        event_type="MOVE",
        version=1,
        task_key="poo@toalha",
        mode="DOWN",
        rate=None,
        size=None,
        human=None,
        iagen=None,
        guide=None,
        other=None,
        alone=None,
        study=None,
    )
    row = transformer.event_to_csv_row(event, "abcd1234")
    
    assert row['timestamp'] == "2025-01-10T10:00:00"
    assert row['student_id'] == "abcd1234"
    assert row['task'] == "toalha"
    assert row['event_type'] == "MOVE"
    assert row['mode'] == "DOWN"
    assert row['rate'] == ''


def test_event_to_csv_row_with_self_data():
    """Testa a conversão de evento SELF com todos os campos."""
    transformer = TKOTransformer("test-salt")
    event = TKOLogEvent(
        timestamp=datetime(2025, 1, 10, 10, 30, 0),
        event_type="SELF",
        version=1,
        task_key="toalha",
        mode=None,
        rate=100,
        size=None,
        human="sim",
        iagen="copilot",
        guide="readme",
        other="google",
        alone=8,
        study=45,
    )
    row = transformer.event_to_csv_row(event, "abcd1234")
    
    assert row['event_type'] == "SELF"
    assert row['rate'] == 100
    assert row['human'] == 'yes'
    assert row['iagen'] == 'github_copilot'
    assert row['guide'] == 'readme'
    assert row['other'] == 'google'
    assert row['alone'] == 8
    assert row['study'] == 45


def test_transform_scan_to_csv(minimal_classroom, tmp_path):
    """Testa a transformação completa de varredura para CSV."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "events.csv"
    total = transformer.transform_scan_to_csv(scan, csv_path)
    
    assert total == 7
    assert csv_path.exists()
    
    # Verificar a estrutura do CSV
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    assert len(rows) == 7
    
    # Verificar a primeira linha
    assert 'timestamp' in rows[0]
    assert 'student_id' in rows[0]
    assert 'task' in rows[0]
    assert 'event_type' in rows[0]
    
    # Verificar a normalização de tarefas (prefixo poo@ removido se presente)
    tasks = [r['task'] for r in rows]
    assert 'toalha' in tasks
    assert 'animal' in tasks
    assert not any(t.startswith('poo@') for t in tasks)


def test_transform_sorts_by_timestamp(minimal_classroom, tmp_path):
    """Testa se o CSV de saída está ordenado por timestamp."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "events.csv"
    transformer.transform_scan_to_csv(scan, csv_path)
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    timestamps = [r['timestamp'] for r in rows]
    assert timestamps == sorted(timestamps)


def test_transform_single_student(minimal_classroom, tmp_path):
    """Testa a transformação de um único estudante."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    student = scan.turmas[0].blocks[0].students[0]
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "student.csv"
    total = transformer.transform_single_student(student, csv_path)
    
    assert total > 0
    assert csv_path.exists()


def test_transform_invalid_student_returns_zero(tmp_path):
    """Testa se a transformação de estudante inválido retorna 0."""
    invalid_student = StudentRepo(
        username="invalid",
        repo_path=Path("/nonexistent"),
        valid=False
    )
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "invalid.csv"
    total = transformer.transform_single_student(invalid_student, csv_path)
    
    assert total == 0


def test_transform_preserves_student_privacy(minimal_classroom, tmp_path):
    """Testa se os IDs dos estudantes são pseudonimizados."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "events.csv"
    transformer.transform_scan_to_csv(scan, csv_path)
    
    with open(csv_path, 'r') as f:
        content = f.read()

    assert "student1" not in content
    assert "student2" not in content
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    student_ids = set(r['student_id'] for r in rows)
    
    # Verificar unicidade dos IDs
    assert len(student_ids) == 2
    
    # Verificar formato dos IDs
    for sid in student_ids:
        assert len(sid) == 8
        assert sid.isalnum()


def test_transform_with_multi_block(minimal_classroom_multi_block, tmp_path):
    """Testa a transformação com múltiplos blocos."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom_multi_block)
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "events.csv"
    total = transformer.transform_scan_to_csv(scan, csv_path)
    
    assert total == 4
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # Verificar existência de eventos de ambos os blocos
    tasks = [r['task'] for r in rows]
    assert 'tarefa1' in tasks
    assert 'tarefa2' in tasks

