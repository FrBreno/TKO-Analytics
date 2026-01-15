"""
Modelagem Pydantic de eventos TKO.

Este módulo define os modelos de dados para eventos de telemetria do TKO,
com validação automática, coerção de tipos e mensagens de erro claras.
"""

from datetime import datetime
from typing import Optional, Literal, Dict
from pydantic import BaseModel, Field, model_validator, ConfigDict


class BaseEvent(BaseModel):
    """
    Evento base com campos comuns a todos os tipos de eventos TKO.
    """
    
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    timestamp: datetime = Field(
        ...,
        description="Timestamp do evento em formato ISO-8601"
    )
    task_id: str = Field(
        ...,
        alias='k',
        min_length=1,
        max_length=100,
        description="Identificador da tarefa/atividade"
    )
    student_name: Optional[str] = Field(
        default=None,
        description="Nome do estudante (opcional, extraído do CSV)"
    )
    version: int = Field(
        default=1,
        alias='v',
        ge=1,
        description="Versão do formato de log"
    )


class ExecEvent(BaseEvent):
    """
    Evento de execução/teste de código.
    
    Representa tentativas de compilação e execução do código do aluno,
    incluindo taxa de acerto e informações de erro.
    """
    # Tipo específico do evento e modo de execução
    event_type: Literal['EXEC'] = 'EXEC'
    mode: Literal['FULL', 'LOCK', 'FREE'] = Field(
        ...,
        description="Modo de execução do código"
    )
    
    rate: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="Taxa de acerto percentual (0-100)"
    )
    size: int = Field(
        ...,
        ge=0,
        description="Número de linhas de código (pode ser 0 para código vazio)"
    )
    error: Optional[Literal['NONE', 'COMP', 'EXEC']] = Field(
        default='NONE',
        description="Tipo de erro ocorrido (COMP=compilação, EXEC=execução, NONE=sem erro)"
    )
    
    @model_validator(mode='after')
    def rate_required_for_test_modes(self) -> 'ExecEvent':
        """
        Validar obrigatoriedade de 'rate' para modos FULL/LOCK.
        
        Raises:
            ValueError: Se rate é None em modo FULL ou LOCK
        """
        if self.mode in ['FULL', 'LOCK'] and self.rate is None:
            raise ValueError(
                f"Field 'rate' is required for mode '{self.mode}'. "
                "Use mode='FREE' if rate is not available."
            )
        return self


class MoveEvent(BaseEvent):
    """
    Evento de navegação entre atividades.
    
    Representa ações de seleção, download e edição de atividades pelo aluno.
    """
    # Tipo específico do evento e ação de navegação
    event_type: Literal['MOVE'] = 'MOVE'
    action: Literal['DOWN', 'PICK', 'BACK', 'EDIT'] = Field(
        ...,
        description="Tipo de ação de navegação"
    )
    
    @classmethod
    def from_mode(cls, mode: str, **kwargs) -> 'MoveEvent':
        """
        Factory method para criar MoveEvent a partir do campo 'mode' no log do TKO.
        
        Args:
            mode: Valor do campo 'mode' no CSV (DOWN, PICK, BACK, EDIT)
            **kwargs: Demais campos (timestamp, task_id, version)
        
        Returns:
            Instância de MoveEvent validada
        """
        return cls(action=mode, **kwargs)


class SelfEvent(BaseEvent):
    """
    Evento de auto-avaliação do aluno.
    
    Captura a percepção do aluno sobre seu desempenho, autonomia
    e fontes de ajuda utilizadas.
    """
    # Tipo específico do evento
    event_type: Literal['SELF'] = 'SELF'
    
    rate: int = Field(
        ...,
        ge=0,
        le=100,
        description="Taxa de acerto auto-reportada (0-100)"
    )
    autonomy: Optional[int] = Field(
        None,
        alias='alone',
        ge=0,
        le=10,
        description="Nível de autonomia (0=dependente, 10=autônomo)"
    )
    help_human: Optional[str] = Field(
        None,
        alias='human',
        max_length=500,
        description="Ajuda de pessoas (colegas, professor)"
    )
    help_iagen: Optional[str] = Field(
        None,
        alias='iagen',
        max_length=500,
        description="Ajuda de IA generativa (ChatGPT, Copilot)"
    )
    help_guide: Optional[str] = Field(
        None,
        alias='guide',
        max_length=500,
        description="Uso de guias e tutoriais"
    )
    help_other: Optional[str] = Field(
        None,
        alias='other',
        max_length=500,
        description="Outras fontes de ajuda"
    )
    
    study_minutes: Optional[int] = Field(
        None,
        alias='study',
        ge=0,
        description="Tempo de estudo em minutos"
    )
    
    def get_help_sources(self) -> Dict[str, str]:
        """
        Retorna dicionário com todas as fontes de ajuda não-nulas.
        
        Returns:
            Dict mapeando tipo de ajuda → descrição
        """
        help_sources = {}
        
        if self.help_human:
            help_sources['human'] = self.help_human
        if self.help_iagen:
            help_sources['iagen'] = self.help_iagen
        if self.help_guide:
            help_sources['guide'] = self.help_guide
        if self.help_other:
            help_sources['other'] = self.help_other
        
        return help_sources
    
    def has_any_help(self) -> bool:
        """
        Verifica se o aluno reportou qualquer tipo de ajuda.
        
        Returns:
            True se alguma fonte de ajuda foi mencionada
        """
        return bool(self.get_help_sources())
