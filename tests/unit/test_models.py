"""
Testes unitários para modelos de eventos Pydantic.

Valida criação, validação e edge cases dos modelos ExecEvent, MoveEvent, SelfEvent.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.models import BaseEvent, ExecEvent, MoveEvent, SelfEvent


class TestBaseEvent:
    """Testes para BaseEvent."""
    
    def test_base_event_valid(self):
        """Testa criação válida de BaseEvent."""
        event = BaseEvent(
            timestamp=datetime(2026, 1, 11, 10, 0, 0),
            k='task001'
        )
        
        assert event.task_id == 'task001'
        assert event.version == 1  # default
        assert event.timestamp.year == 2026
    
    def test_base_event_with_alias(self):
        """Testa que aliases funcionam (k → task_id)."""
        event = BaseEvent(
            timestamp=datetime.now(),
            task_id='task002',  # usando nome real
            v=2
        )
        
        assert event.task_id == 'task002'
        assert event.version == 2
    
    def test_base_event_missing_required_field(self):
        """Testa que campos obrigatórios não podem faltar."""
        with pytest.raises(ValidationError) as exc_info:
            BaseEvent(timestamp=datetime.now())  # falta task_id
        
        errors = exc_info.value.errors()
        assert any(e['loc'] == ('task_id',) or e['loc'] == ('k',) for e in errors)
    
    def test_base_event_strips_whitespace(self):
        """Testa que whitespace é removido de strings."""
        event = BaseEvent(
            timestamp=datetime.now(),
            k='  task003  '
        )
        
        assert event.task_id == 'task003'


class TestExecEvent:
    """Testes para ExecEvent."""
    
    def test_exec_event_full_mode_valid(self):
        """Testa EXEC event válido em modo FULL."""
        event = ExecEvent(
            timestamp=datetime(2026, 1, 11, 10, 30, 0),
            k='calculadora',
            mode='FULL',
            rate=75,
            size=100
        )
        
        assert event.event_type == 'EXEC'
        assert event.mode == 'FULL'
        assert event.rate == 75
        assert event.size == 100
        assert event.error == 'NONE'  # default
    
    def test_exec_event_rate_required_for_full(self):
        """Testa que rate é obrigatório para modo FULL."""
        with pytest.raises(ValidationError) as exc_info:
            ExecEvent(
                timestamp=datetime.now(),
                k='task001',
                mode='FULL',
                size=50
                # rate missing!
            )
        
        error_msg = str(exc_info.value)
        assert 'rate' in error_msg.lower()
        assert 'required' in error_msg.lower()
    
    def test_exec_event_rate_required_for_lock(self):
        """Testa que rate é obrigatório para modo LOCK."""
        with pytest.raises(ValidationError) as exc_info:
            ExecEvent(
                timestamp=datetime.now(),
                k='task001',
                mode='LOCK',
                size=50
            )
        
        error_msg = str(exc_info.value)
        assert 'rate' in error_msg.lower()
    
    def test_exec_event_free_mode_without_rate(self):
        """Testa que modo FREE aceita rate=None."""
        event = ExecEvent(
            timestamp=datetime.now(),
            k='task001',
            mode='FREE',
            size=75
        )
        
        assert event.mode == 'FREE'
        assert event.rate is None
    
    def test_exec_event_rate_range_validation(self):
        """Testa validação de range do rate (0-100)."""
        # Rate negativo
        with pytest.raises(ValidationError):
            ExecEvent(
                timestamp=datetime.now(),
                k='task001',
                mode='FULL',
                rate=-10,
                size=50
            )
        
        # Rate > 100
        with pytest.raises(ValidationError):
            ExecEvent(
                timestamp=datetime.now(),
                k='task001',
                mode='FULL',
                rate=150,
                size=50
            )
    
    def test_exec_event_size_must_be_positive(self):
        """Testa que size deve ser > 0."""
        with pytest.raises(ValidationError):
            ExecEvent(
                timestamp=datetime.now(),
                k='task001',
                mode='FULL',
                rate=100,
                size=0  # inválido
            )
    
    def test_exec_event_with_error(self):
        """Testa EXEC event com erro de compilação."""
        event = ExecEvent(
            timestamp=datetime.now(),
            k='task001',
            mode='FULL',
            rate=0,
            size=30,
            error='COMP'
        )
        
        assert event.error == 'COMP'
        assert event.rate == 0


class TestMoveEvent:
    """Testes para MoveEvent."""
    
    def test_move_event_pick_action(self):
        """Testa MOVE event com ação PICK."""
        event = MoveEvent(
            timestamp=datetime.now(),
            k='xadrez',
            action='PICK'
        )
        
        assert event.event_type == 'MOVE'
        assert event.action == 'PICK'
        assert event.task_id == 'xadrez'
    
    def test_move_event_from_mode_factory(self):
        """Testa factory method from_mode."""
        event = MoveEvent.from_mode(
            mode='DOWN',
            timestamp=datetime(2026, 1, 11, 9, 0, 0),
            k='ponto'
        )
        
        assert event.action == 'DOWN'
        assert event.task_id == 'ponto'
    
    def test_move_event_invalid_action(self):
        """Testa que ações inválidas são rejeitadas."""
        with pytest.raises(ValidationError):
            MoveEvent(
                timestamp=datetime.now(),
                k='task001',
                action='INVALID'  # não está em ['DOWN', 'PICK', 'BACK', 'EDIT']
            )
    
    def test_move_event_all_actions(self):
        """Testa criação com todas as ações válidas."""
        actions = ['DOWN', 'PICK', 'BACK', 'EDIT']
        
        for action in actions:
            event = MoveEvent(
                timestamp=datetime.now(),
                k='task001',
                action=action
            )
            assert event.action == action


class TestSelfEvent:
    """Testes para SelfEvent."""
    
    def test_self_event_minimal(self):
        """Testa SELF event com campos mínimos."""
        event = SelfEvent(
            timestamp=datetime.now(),
            k='task001',
            rate=100
        )
        
        assert event.event_type == 'SELF'
        assert event.rate == 100
        assert event.autonomy is None
    
    def test_self_event_full_with_help(self):
        """Testa SELF event completo com múltiplas fontes de ajuda."""
        event = SelfEvent(
            timestamp=datetime.now(),
            k='calculadora',
            rate=80,
            alone=7,  # usando alias
            human='professor explicou algoritmo',
            iagen='ChatGPT ajudou com sintaxe',
            guide='consultei documentação oficial',
            study=120
        )
        
        assert event.autonomy == 7
        assert event.help_human == 'professor explicou algoritmo'
        assert event.help_iagen == 'ChatGPT ajudou com sintaxe'
        assert event.help_guide == 'consultei documentação oficial'
        assert event.study_minutes == 120
    
    def test_self_event_autonomy_range(self):
        """Testa validação de range do autonomy (0-10)."""
        # Autonomy negativo
        with pytest.raises(ValidationError):
            SelfEvent(
                timestamp=datetime.now(),
                k='task001',
                rate=50,
                alone=-1
            )
        
        # Autonomy > 10
        with pytest.raises(ValidationError):
            SelfEvent(
                timestamp=datetime.now(),
                k='task001',
                rate=50,
                alone=15
            )
    
    def test_self_event_rate_range(self):
        """Testa validação de range do rate (0-100)."""
        with pytest.raises(ValidationError):
            SelfEvent(
                timestamp=datetime.now(),
                k='task001',
                rate=150  # > 100
            )
    
    def test_self_event_study_minutes_non_negative(self):
        """Testa que study_minutes não pode ser negativo."""
        with pytest.raises(ValidationError):
            SelfEvent(
                timestamp=datetime.now(),
                k='task001',
                rate=100,
                study=-30
            )
    
    def test_get_help_sources_empty(self):
        """Testa get_help_sources quando não há ajuda."""
        event = SelfEvent(
            timestamp=datetime.now(),
            k='task001',
            rate=100,
            alone=10
        )
        
        help_sources = event.get_help_sources()
        assert help_sources == {}
        assert not event.has_any_help()
    
    def test_get_help_sources_multiple(self):
        """Testa get_help_sources com múltiplas fontes."""
        event = SelfEvent(
            timestamp=datetime.now(),
            k='task001',
            rate=75,
            alone=5,
            human='colega',
            iagen='copilot',
            guide='youtube'
        )
        
        help_sources = event.get_help_sources()
        assert len(help_sources) == 3
        assert help_sources['human'] == 'colega'
        assert help_sources['iagen'] == 'copilot'
        assert help_sources['guide'] == 'youtube'
        assert event.has_any_help()
    
    def test_has_any_help_true(self):
        """Testa has_any_help quando há ajuda."""
        event = SelfEvent(
            timestamp=datetime.now(),
            k='task001',
            rate=50,
            other='stackoverflow'
        )
        
        assert event.has_any_help()


class TestEventsSerialization:
    """Testes de serialização e JSON."""
    
    def test_exec_event_to_dict(self):
        """Testa conversão para dict."""
        event = ExecEvent(
            timestamp=datetime(2026, 1, 11, 10, 0, 0),
            k='task001',
            mode='FULL',
            rate=100,
            size=50
        )
        
        data = event.model_dump()
        assert data['task_id'] == 'task001'
        assert data['mode'] == 'FULL'
        assert data['rate'] == 100
    
    def test_exec_event_to_json(self):
        """Testa conversão para JSON."""
        event = ExecEvent(
            timestamp=datetime(2026, 1, 11, 10, 0, 0),
            k='task001',
            mode='FULL',
            rate=75,
            size=100
        )
        
        json_str = event.model_dump_json()
        assert 'task001' in json_str
        assert '"rate":75' in json_str or '"rate": 75' in json_str
    
    def test_exec_event_from_dict(self):
        """Testa reconstrução a partir de dict."""
        data = {
            'timestamp': '2026-01-11T10:00:00',
            'task_id': 'task001',
            'mode': 'FULL',
            'rate': 50,
            'size': 80
        }
        
        event = ExecEvent(**data)
        assert event.task_id == 'task001'
        assert event.rate == 50
