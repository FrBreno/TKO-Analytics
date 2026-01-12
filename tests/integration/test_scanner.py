"""
Testes de integração para ClassroomScanner.

Testa descoberta de estrutura de sala de aula, detecção de subdiretórios e geração de avisos.
"""

from src.tko_integration.scanner import ClassroomScanner


def test_scanner_finds_turma(minimal_classroom):
    """Testa se o scanner encontra diretório de turma."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    assert len(scan.turmas) == 1
    assert scan.turmas[0].name == "turma_test"


def test_scanner_finds_block(minimal_classroom):
    """Testa se o scanner encontra diretório de bloco."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    assert len(scan.turmas[0].blocks) == 1
    assert scan.turmas[0].blocks[0].name == "Bloco A"


def test_scanner_finds_all_students(minimal_classroom):
    """Testa se o scanner encontra todos os repositórios de estudantes."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    students = scan.turmas[0].blocks[0].students
    
    assert len(students) == 3
    assert scan.total_students == 3
    assert scan.total_repos == 3


def test_scanner_identifies_valid_repos(minimal_classroom):
    """Testa se o scanner identifica corretamente repos válidos com .tko/."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    assert scan.valid_repos == 2


def test_scanner_finds_poo_subdir(minimal_classroom):
    """Testa se o scanner encontra subdiretório poo/."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    students = scan.turmas[0].blocks[0].students
    student1 = [s for s in students if s.username == "student1"][0]
    
    assert student1.valid
    assert student1.tko_subdir.name == "poo"
    assert student1.tko_dir == student1.tko_subdir / ".tko"


def test_scanner_finds_myrep_subdir(minimal_classroom):
    """Testa se o scanner encontra subdiretório myrep/ (variação)."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    students = scan.turmas[0].blocks[0].students
    student2 = [s for s in students if s.username == "student2"][0]
    
    assert student2.valid
    assert student2.tko_subdir.name == "myrep"


def test_scanner_detects_missing_tko(minimal_classroom):
    """Testa se o scanner detecta repos sem .tko/."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    students = scan.turmas[0].blocks[0].students
    student3 = [s for s in students if s.username == "student3"][0]
    
    assert not student3.valid
    assert student3.warning == "No .tko/ directory found"


def test_scanner_generates_warnings(minimal_classroom):
    """Testa se o scanner gera aviso para .tko/ ausente."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    assert len(scan.warnings) == 1
    assert "student3" in scan.warnings[0]
    assert "No .tko/" in scan.warnings[0]


def test_scanner_multi_block(minimal_classroom_multi_block):
    """Testa se o scanner lida com múltiplos blocos (A e B)."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom_multi_block)
    
    assert len(scan.turmas) == 1
    assert len(scan.turmas[0].blocks) == 2
    
    blocks = scan.turmas[0].blocks
    block_names = [b.name for b in blocks]
    
    assert "Bloco A" in block_names
    assert "Bloco B" in block_names


def test_scanner_extracts_usernames(minimal_classroom_multi_block):
    """Testa se o scanner extrai nomes de usuário dos nomes dos repos."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom_multi_block)
    
    all_students = []
    for block in scan.turmas[0].blocks:
        all_students.extend(block.students)
    
    usernames = [s.username for s in all_students]
    
    assert "alice" in usernames
    assert "bob" in usernames


def test_scanner_empty_directory(tmp_path):
    """Testa se o scanner lida com diretório vazio graciosamente."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(tmp_path)
    
    assert len(scan.turmas) == 0
    assert scan.total_students == 0
    assert len(scan.warnings) >= 1


def test_scanner_nonexistent_directory(tmp_path):
    """Testa se o scanner lida com diretório inexistente."""
    scanner = ClassroomScanner()
    nonexistent = tmp_path / "does_not_exist"
    scan = scanner.scan_directory(nonexistent)
    
    assert len(scan.turmas) == 0
    assert "does not exist" in scan.warnings[0]
