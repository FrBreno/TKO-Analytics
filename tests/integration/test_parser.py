"""
Testes de integração para LogParser e RepositoryParser.

Testa o parsing de arquivos de log TKO, repository.yaml e dados de rastreamento.
"""

from datetime import datetime
from src.tko_integration.parser import (
    LogParser, 
    RepositoryParser, 
    TrackingParser,
    ValueNormalizer
)


def test_parse_move_event():
    """Testa parsing de evento MOVE."""
    line = "2025-09-18 02:44:25, MOVE, v:1, k:toalha, mode:DOWN"
    event = LogParser.parse_log_line(line)
    
    assert event is not None
    assert event.timestamp == datetime(2025, 9, 18, 2, 44, 25)
    assert event.event_type == "MOVE"
    assert event.task_key == "toalha"
    assert event.mode == "DOWN"
    assert event.rate is None


def test_parse_exec_event():
    """Testa parsing de evento EXEC."""
    line = "2025-09-18 02:44:34, EXEC, v:1, k:toalha, mode:LOCK, rate:100, size:25"
    event = LogParser.parse_log_line(line)
    
    assert event.event_type == "EXEC"
    assert event.mode == "LOCK"
    assert event.rate == 100
    assert event.size == 25


def test_parse_self_event():
    """Testa parsing de evento SELF com dados de autoavaliação."""
    line = "2025-09-16 19:53:28, SELF, v:1, k:toalha, rate:100, guide:sena_e_readme, other:google, alone:9, study:15"
    event = LogParser.parse_log_line(line)
    
    assert event.event_type == "SELF"
    assert event.rate == 100
    assert event.guide == "sena_e_readme"
    assert event.other == "google"
    assert event.alone == 9
    assert event.study == 15


def test_parse_self_event_with_human_ai():
    """Testa parsing de evento SELF com campos human e AI."""
    line = "2025-09-16 19:53:28, SELF, v:1, k:toalha, rate:100, human:sim_amigo, iagen:copilot, alone:8, study:45"
    event = LogParser.parse_log_line(line)
    
    assert event.human == "sim_amigo"
    assert event.iagen == "copilot"


def test_parse_malformed_line():
    """Testa que o parser lida com linhas malformadas graciosamente."""
    line = "invalid line format"
    event = LogParser.parse_log_line(line)
    
    assert event is None


def test_parse_empty_line():
    """Testa que o parser lida com linhas vazias."""
    event = LogParser.parse_log_line("")
    assert event is None


def test_parse_log_file(minimal_classroom):
    """Testa parsing de arquivo de log completo."""
    student1_log = minimal_classroom / "bloco-a-submissions" / "bloco-a-student1" / "poo" / ".tko" / "log" / "2025-01-10.log"
    events = LogParser.parse_log_file(student1_log)
    
    assert len(events) == 4
    assert events[0].event_type == "MOVE"
    assert events[1].event_type == "MOVE"
    assert events[2].event_type == "EXEC"
    assert events[3].event_type == "SELF"


def test_parse_all_logs(minimal_classroom):
    """Testa parsing de todos os logs no diretório."""
    log_dir = minimal_classroom / "bloco-a-submissions" / "bloco-a-student1" / "poo" / ".tko" / "log"
    events = LogParser.parse_all_logs(log_dir)
    
    assert len(events) == 4
    assert events[0].timestamp <= events[1].timestamp


def test_parse_repository_yaml(minimal_classroom):
    """Testa parsing de tarefas do repository.yaml."""
    repo_yaml = minimal_classroom / "bloco-a-submissions" / "bloco-a-student1" / "poo" / ".tko" / "repository.yaml"
    tasks = RepositoryParser.parse_repository_yaml(repo_yaml)
    
    assert "toalha" in tasks
    task = tasks["toalha"]
    
    assert task.rate == 100
    assert task.human == "nao"
    assert task.iagen == "copilot"
    assert task.guide == "readme"
    assert task.other == "google"
    assert task.alone == 8
    assert task.study == 45


def test_parse_task_value_string():
    """Testa parsing de string de valor de tarefa."""
    value_str = "{rate:100, human:SIM, alone:6, study:120}"
    parsed = RepositoryParser.parse_task_value(value_str)
    
    assert parsed["rate"] == 100
    assert parsed["human"] == "SIM"
    assert parsed["alone"] == 6
    assert parsed["study"] == 120


def test_parse_task_value_with_quotes():
    """Testa parsing de valor de tarefa com aspas externas."""
    value_str = "'{rate:80, guide:sena_e_readme, alone:10, study:60}'"
    parsed = RepositoryParser.parse_task_value(value_str)
    
    assert parsed["rate"] == 80
    assert parsed["guide"] == "sena_e_readme"


def test_parse_draft_json(minimal_classroom):
    """Testa parsing de dados de rastreamento draft.py.json."""
    draft_file = minimal_classroom / "bloco-a-submissions" / "bloco-a-student1" / "poo" / ".tko" / "track" / "toalha" / "draft.py.json"
    snapshot = TrackingParser.parse_draft_json(draft_file, "toalha")
    
    assert snapshot is not None
    assert snapshot.task_key == "toalha"
    assert "def toalha()" in snapshot.code
    assert snapshot.size > 0


def test_parse_track_csv(minimal_classroom):
    """Testa parsing do histórico track.csv."""
    track_csv = minimal_classroom / "bloco-a-submissions" / "bloco-a-student1" / "poo" / ".tko" / "track" / "toalha" / "track.csv"
    snapshots = TrackingParser.parse_track_csv(track_csv, "toalha")
    
    assert len(snapshots) == 3
    assert all(s.task_key == "toalha" for s in snapshots)


def test_normalize_human_values():
    """Testa normalização do campo de ajuda humana."""
    assert ValueNormalizer.normalize_human("sim") == "yes"
    assert ValueNormalizer.normalize_human("SIM") == "yes"
    assert ValueNormalizer.normalize_human("nao") == "none"
    assert ValueNormalizer.normalize_human("_amigo") == "friend"
    assert ValueNormalizer.normalize_human("sim_amigo_monitor") == "friend_monitor"


def test_normalize_ai_values():
    """Testa normalização do campo de uso de IA."""
    assert ValueNormalizer.normalize_ai("copilot") == "github_copilot"
    assert ValueNormalizer.normalize_ai("_gpt") == "chatgpt"
    assert ValueNormalizer.normalize_ai("copilot_e_gpt") == "copilot_chatgpt"
    assert ValueNormalizer.normalize_ai("gemini") == "google_gemini"


def test_normalize_guide_values():
    """Testa normalização do campo de material guia."""
    assert ValueNormalizer.normalize_guide("sena") == "professor"
    assert ValueNormalizer.normalize_guide("readme") == "readme"
    assert ValueNormalizer.normalize_guide("sena_e_readme") == "professor_readme"
    assert ValueNormalizer.normalize_guide("_video") == "video"
    assert ValueNormalizer.normalize_guide("Aula") == "class"


def test_normalize_preserves_unknown_values():
    """Testa que o normalizador preserva valores não mapeados."""
    assert ValueNormalizer.normalize_human("unknown_value") == "unknown_value"
    assert ValueNormalizer.normalize_ai("new_ai_tool") == "new_ai_tool"
