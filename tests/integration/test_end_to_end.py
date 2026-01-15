"""
End-to-end integration tests.

Tests complete pipeline: scan → parse → transform → validate.
"""

from src.tko_integration.scanner import ClassroomScanner
from src.tko_integration.transformer import TKOTransformer
from src.tko_integration.validator import DataValidator


def test_complete_pipeline(minimal_classroom, tmp_path):
    """Test complete pipeline from scan to CSV."""
    # Criar scan com minimal_classroom
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    
    assert scan.total_students == 3
    assert scan.valid_repos == 2
    
    # Validar scan
    validator = DataValidator()
    warnings = validator.validate_scan(scan)
    
    assert len(warnings) >= 1
    
    # Transformar para CSV
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "events.csv"
    total = transformer.transform_scan_to_csv(scan, csv_path)
    
    assert total == 7
    assert csv_path.exists()
    
    # Verificar se o CSV pode ser lido corretamente
    import csv
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    assert len(rows) == 7


def test_pipeline_generates_validation_report(minimal_classroom):
    """Test pipeline generates comprehensive validation report."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom)
    validator = DataValidator()
    report = validator.generate_report(scan)
    
    assert "TKO DATA VALIDATION REPORT" in report
    assert "SUMMARY:" in report
    assert "Valid Repos: 2/3" in report
    assert "WARNINGS" in report


def test_pipeline_with_multi_block(minimal_classroom_multi_block, tmp_path):
    """Test pipeline with multiple blocks."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(minimal_classroom_multi_block)
    
    assert len(scan.turmas[0].blocks) == 2
    
    # Transform to CSV
    transformer = TKOTransformer("test-salt")
    csv_path = tmp_path / "events.csv"
    total = transformer.transform_scan_to_csv(scan, csv_path)
    
    assert total == 4


def test_pipeline_handles_empty_directory(tmp_path):
    """Test pipeline handles empty directory gracefully."""
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(tmp_path)
    validator = DataValidator()
    warnings = validator.validate_scan(scan)
    
    assert len(warnings) >= 1
    assert "No turmas" in warnings[0]


def test_pipeline_consistency(minimal_classroom, tmp_path):
    """Test that running pipeline twice produces same results."""
    scanner = ClassroomScanner()
    transformer = TKOTransformer("test-salt")
    
    # Primeira execução
    scan1 = scanner.scan_directory(minimal_classroom)
    csv1 = tmp_path / "events1.csv"
    total1 = transformer.transform_scan_to_csv(scan1, csv1)
    
    # Segunda execução
    scan2 = scanner.scan_directory(minimal_classroom)
    csv2 = tmp_path / "events2.csv"
    total2 = transformer.transform_scan_to_csv(scan2, csv2)
    
    # Primeira exec = Segunda exec
    assert total1 == total2
    assert csv1.read_text() == csv2.read_text()
