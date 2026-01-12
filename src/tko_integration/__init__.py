"""
Módulo de Integração TKO

Este módulo fornece ferramentas para importar e processar dados TKO de repositórios do GitHub Classroom.

Componentes:
- scanner: Descobre a estrutura da sala de aula (turmas, blocos, estudantes)
- parser: Analisa arquivos de log TKO e repository.yaml
- transformer: Converte o formato TKO para CSV do TKO-Analytics
- validator: Valida a integridade dos dados e gera avisos
"""

from .scanner import ClassroomScanner, StudentRepo, ClassroomScan, Turma, Block
from .parser import LogParser, RepositoryParser, TKOLogEvent, TKOTaskData, TrackingParser
from .transformer import TKOTransformer
from .validator import DataValidator

__all__ = [
    'ClassroomScanner',
    'StudentRepo',
    'ClassroomScan',
    'Turma',
    'Block',
    'LogParser',
    'RepositoryParser',
    'TKOLogEvent',
    'TKOTaskData',
    'TrackingParser',
    'TKOTransformer',
    'DataValidator',
]
