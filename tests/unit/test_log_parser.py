"""
Testes para LogParser.

Testa parsing de arquivos CSV TKO em modelos Pydantic validados.
"""

import csv
import pytest
from pathlib import Path

from src.parsers import LogParser
from src.parsers.log_parser import ParseError
from src.models import ExecEvent, MoveEvent, SelfEvent


@pytest.fixture
def temp_csv(tmp_path):
    """Fixture para criar arquivos CSV temporários."""
    def _make_csv(lines: list[dict]) -> Path:
        filepath = tmp_path / "test.csv"
        
        if lines:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=lines[0].keys())
                writer.writeheader()
                writer.writerows(lines)
        else:
            # CSV vazio
            filepath.write_text("")
        
        return filepath
    return _make_csv


class TestLogParserExecEvents:
    """Testes para parsing de ExecEvent."""
    
    def test_parse_exec_full_mode(self, temp_csv):
        """Testa parsing de evento FULL com rate."""
        lines = [{
            'timestamp': '2024-01-15T10:30:00',
            'task': 'task_001',
            'mode': 'FULL',
            'rate': '85',
            'size': '120',
            'error': 'NONE'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert len(events) == 1
        assert isinstance(events[0], ExecEvent)
        assert events[0].mode == 'FULL'
        assert events[0].rate == 85
        assert events[0].size == 120
        assert events[0].error == 'NONE'
    
    def test_parse_exec_free_mode_no_rate(self, temp_csv):
        """Testa parsing de FREE sem rate (válido)."""
        lines = [{
            'timestamp': '2024-01-15T10:30:00',
            'task': 'task_002',
            'mode': 'FREE',
            'rate': '',  # rate vazio é OK para FREE
            'size': '50',
            'error': 'NONE'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert len(events) == 1
        assert events[0].mode == 'FREE'
        assert events[0].rate is None
    
    def test_parse_exec_with_error(self, temp_csv):
        """Testa parsing com campo error."""
        lines = [{
            'timestamp': '2024-01-15T10:35:00',
            'task': 'task_003',
            'mode': 'FULL',
            'rate': '50',
            'size': '80',
            'error': 'COMP'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert events[0].error == 'COMP'
    
    def test_parse_exec_invalid_rate_for_full(self, temp_csv):
        """Testa que FULL sem rate falha na validação Pydantic."""
        lines = [{
            'timestamp': '2024-01-15T10:30:00',
            'task': 'task_004',
            'mode': 'FULL',
            'rate': '',  # rate vazio para FULL deve falhar
            'size': '100',
            'error': 'NONE'
        }]
        
        parser = LogParser(strict=True)
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse_file(temp_csv(lines))


class TestLogParserMoveEvents:
    """Testes para parsing de MoveEvent."""
    
    def test_parse_move_down(self, temp_csv):
        """Testa parsing de evento DOWN."""
        lines = [{
            'timestamp': '2024-01-15T11:00:00',
            'task': 'task_010',
            'mode': 'DOWN'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert len(events) == 1
        assert isinstance(events[0], MoveEvent)
        assert events[0].action == 'DOWN'
    
    def test_parse_all_move_actions(self, temp_csv):
        """Testa parsing de todas as ações de movimento."""
        lines = [
            {'timestamp': '2024-01-15T11:00:00', 'task': 'task_010', 'mode': 'DOWN'},
            {'timestamp': '2024-01-15T11:01:00', 'task': 'task_011', 'mode': 'PICK'},
            {'timestamp': '2024-01-15T11:02:00', 'task': 'task_012', 'mode': 'BACK'},
            {'timestamp': '2024-01-15T11:03:00', 'task': 'task_013', 'mode': 'EDIT'},
        ]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert len(events) == 4
        assert all(isinstance(e, MoveEvent) for e in events)
        assert [e.action for e in events] == ['DOWN', 'PICK', 'BACK', 'EDIT']


class TestLogParserSelfEvents:
    """Testes para parsing de SelfEvent."""
    
    def test_parse_self_minimal(self, temp_csv):
        """Testa parsing de SELF com campos mínimos."""
        lines = [{
            'timestamp': '2024-01-15T12:00:00',
            'task': 'task_020',
            'mode': 'SELF',
            'rate': '90',
            'autonomy': '8',
            'help_human': '',
            'help_iagen': '',
            'help_guide': '',
            'help_other': '',
            'study': '0'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert len(events) == 1
        assert isinstance(events[0], SelfEvent)
        assert events[0].rate == 90
        assert events[0].autonomy == 8
        assert events[0].has_any_help() is False
    
    def test_parse_self_with_help(self, temp_csv):
        """Testa parsing de SELF com ajuda."""
        lines = [{
            'timestamp': '2024-01-15T12:30:00',
            'task': 'task_021',
            'mode': 'SELF',
            'rate': '75',
            'autonomy': '5',
            'help_human': 'professor_colega',  # Texto descrevendo ajuda
            'help_iagen': 'gpt_copilot',       # Texto descrevendo ajuda de IA
            'help_guide': '',
            'help_other': '',
            'study': '120'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert events[0].help_human == 'professor_colega'
        assert events[0].help_iagen == 'gpt_copilot'
        assert events[0].help_guide is None
        assert events[0].help_other is None
        assert events[0].has_any_help() is True
        assert events[0].study_minutes == 120
    
    def test_parse_bool_variations(self, temp_csv):
        """Testa parsing de variações de help strings."""
        lines = [{
            'timestamp': '2024-01-15T12:30:00',
            'task': 'task_022',
            'mode': 'SELF',
            'rate': '80',
            'autonomy': '7',
            'help_human': 'amigo_namorado',  # String de ajuda
            'help_iagen': 'gpt',              # String de ajuda de IA
            'help_guide': '',                 # Vazio = None
            'help_other': '',                 # Vazio = None
            'study': '60'
        }]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert events[0].help_human == 'amigo_namorado'
        assert events[0].help_iagen == 'gpt'
        assert events[0].help_guide is None
        assert events[0].help_other is None


class TestLogParserErrorHandling:
    """Testes para tratamento de erros."""
    
    def test_missing_timestamp(self, temp_csv):
        """Testa erro quando timestamp está ausente."""
        lines = [{
            'timestamp': '',  # timestamp vazio
            'task': 'task_030',
            'mode': 'FULL',
            'rate': '80',
            'size': '100',
            'error': 'NONE'
        }]
        
        parser = LogParser(strict=True)
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse_file(temp_csv(lines))
        assert "timestamp" in str(exc_info.value).lower()
    
    def test_missing_task(self, temp_csv):
        """Testa erro quando task está ausente."""
        lines = [{
            'timestamp': '2024-01-15T10:30:00',
            'task': '',  # task vazio
            'mode': 'FULL',
            'rate': '80',
            'size': '100',
            'error': 'NONE'
        }]
        
        parser = LogParser(strict=True)
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse_file(temp_csv(lines))
        assert "task" in str(exc_info.value).lower()
    
    def test_unknown_mode(self, temp_csv):
        """Testa erro com mode desconhecido."""
        lines = [{
            'timestamp': '2024-01-15T10:30:00',
            'task': 'task_031',
            'mode': 'INVALID',  # mode inválido
            'rate': '80',
            'size': '100',
            'error': 'NONE'
        }]
        
        parser = LogParser(strict=True)
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse_file(temp_csv(lines))
        assert "mode" in str(exc_info.value).lower()
    
    def test_non_strict_mode_collects_errors(self, temp_csv):
        """Testa que modo não-strict coleta erros sem travar."""
        lines = [
            {'timestamp': '2024-01-15T10:30:00', 'task': 'task_040', 'mode': 'FULL', 'rate': '80', 'size': '100', 'error': 'NONE'},
            {'timestamp': '', 'task': 'task_041', 'mode': 'FULL', 'rate': '80', 'size': '100', 'error': 'NONE'},  # timestamp inválido
            {'timestamp': '2024-01-15T10:32:00', 'task': 'task_042', 'mode': 'FREE', 'rate': '', 'size': '50', 'error': 'NONE'},
        ]
        
        parser = LogParser(strict=False)
        events = parser.parse_file(temp_csv(lines))
        
        # Deve ter parseado 2 eventos válidos
        assert len(events) == 2
        # Deve ter coletado 1 erro
        assert len(parser.errors) == 1
        assert "timestamp" in parser.errors[0].reason.lower()
    
    def test_file_not_found(self):
        """Testa erro quando arquivo não existe."""
        parser = LogParser()
        
        with pytest.raises(FileNotFoundError):
            parser.parse_file("nonexistent_file.csv")


class TestLogParserMixedEvents:
    """Testes com múltiplos tipos de eventos."""
    
    def test_parse_mixed_event_types(self, temp_csv):
        """Testa parsing de arquivo com múltiplos tipos de eventos."""
        # Primeira linha define TODOS os campos possíveis
        lines = [
            {'timestamp': '2024-01-15T10:00:00', 'task': 'task_050', 'mode': 'DOWN', 
             'rate': '', 'size': '', 'error': '', 'autonomy': '', 
             'help_human': '', 'help_iagen': '', 'help_guide': '', 'help_other': '', 'study': ''},
            {'timestamp': '2024-01-15T10:01:00', 'task': 'task_050', 'mode': 'PICK',
             'rate': '', 'size': '', 'error': '', 'autonomy': '', 
             'help_human': '', 'help_iagen': '', 'help_guide': '', 'help_other': '', 'study': ''},
            {'timestamp': '2024-01-15T10:02:00', 'task': 'task_050', 'mode': 'FULL', 
             'rate': '90', 'size': '150', 'error': 'NONE', 'autonomy': '', 
             'help_human': '', 'help_iagen': '', 'help_guide': '', 'help_other': '', 'study': ''},
            {'timestamp': '2024-01-15T10:05:00', 'task': 'task_050', 'mode': 'FULL', 
             'rate': '100', 'size': '150', 'error': 'NONE', 'autonomy': '', 
             'help_human': '', 'help_iagen': '', 'help_guide': '', 'help_other': '', 'study': ''},
            {'timestamp': '2024-01-15T10:06:00', 'task': 'task_050', 'mode': 'BACK',
             'rate': '', 'size': '', 'error': '', 'autonomy': '', 
             'help_human': '', 'help_iagen': '', 'help_guide': '', 'help_other': '', 'study': ''},
            {'timestamp': '2024-01-15T10:10:00', 'task': 'task_050', 'mode': 'SELF', 
             'rate': '95', 'size': '', 'error': '', 'autonomy': '9', 
             'help_human': '', 'help_iagen': '', 'help_guide': '', 'help_other': '', 'study': '30'},
        ]
        
        parser = LogParser()
        events = parser.parse_file(temp_csv(lines))
        
        assert len(events) == 6
        
        # Verifica tipos
        assert isinstance(events[0], MoveEvent)
        assert isinstance(events[1], MoveEvent)
        assert isinstance(events[2], ExecEvent)
        assert isinstance(events[3], ExecEvent)
        assert isinstance(events[4], MoveEvent)
        assert isinstance(events[5], SelfEvent)
        
        # Verifica ordem temporal
        assert events[0].timestamp < events[-1].timestamp
    
    def test_empty_csv(self, temp_csv):
        """Testa parsing de CSV vazio."""
        parser = LogParser()
        events = parser.parse_file(temp_csv([]))
        
        assert events == []
        assert parser.errors == []
