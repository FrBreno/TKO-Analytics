"""
Parser de logs CSV do TKO.

Este módulo implementa o parser de arquivos CSV de telemetria TKO,
convertendo linhas CSV em modelos Pydantic validados.
"""

import csv
import structlog
from pathlib import Path
from datetime import datetime
from typing import List, Union, Optional

from tko_analytics.models import BaseEvent, ExecEvent, MoveEvent, SelfEvent

logger = structlog.get_logger()

class ParseError(Exception):
    """Erro ao parsear linha CSV."""
    
    def __init__(self, line_num: int, raw_line: str, reason: str):
        self.line_num = line_num
        self.raw_line = raw_line
        self.reason = reason
        super().__init__(f"Line {line_num}: {reason}")


class LogParser:
    """
    Parser de arquivos CSV TKO.
    
    Converte logs CSV do formato TKO em modelos Pydantic validados,
    com suporte a múltiplos tipos de eventos (exec, move, self).
    """
    
    def __init__(self, strict: bool = True):
        """
        Inicializa o parser.
        
        Args:
            strict: Se True, lança exceção em linhas inválidas.
                   Se False, registra erro e continua.
        """
        self.strict = strict
        self.errors: List[ParseError] = []
        
    def parse_file(self, filepath: Union[str, Path]) -> List[BaseEvent]:
        """
        Parseia arquivo CSV completo.
        
        Args:
            filepath: Caminho do arquivo CSV
            
        Returns:
            Lista de eventos parseados e validados
            
        Raises:
            FileNotFoundError: Se arquivo não existe
            ParseError: Se strict=True e houver erro de parse
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        self.errors.clear()
        events = []
        
        logger.info("[LogParser.parse_file] - parsing_csv", file=str(filepath))
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for _, row in enumerate(reader, start=2):  # start=2 (pula cabeçalho - linha 1)
                try:
                    event = self._parse_line(row)
                    if event:
                        events.append(event)
                except ParseError as e:
                    if self.strict:
                        raise
                    logger.warning("[LogParser.parse_file] parse_error", **e.__dict__)
                    self.errors.append(e)
                    
        logger.info("[LogParser.parse_file] - parsing_complete", 
                   events=len(events), 
                   errors=len(self.errors))
        
        return events
    
    def _parse_line(self, row: dict) -> Optional[BaseEvent]:
        """
        Parseia uma linha CSV em evento Pydantic.
        
        Args:
            row: Dicionário com campos CSV
            
        Returns:
            Instância de ExecEvent, MoveEvent ou SelfEvent
            None se linha deve ser ignorada
            
        Raises:
            ParseError: Se linha está malformada
        """
        # Extrai campos comuns (BaseEvent)
        try:
            timestamp_str = row.get('timestamp', '').strip()
            timestamp = datetime.fromisoformat(timestamp_str)
        except (ValueError, KeyError) as e:
            raise ParseError(
                line_num=0,
                raw_line=str(row),
                reason=f"Invalid timestamp: {e}"
            )
        
        task_id = row.get('task', '').strip()
        if not task_id:
            raise ParseError(
                line_num=0,
                raw_line=str(row),
                reason="Missing required field 'task'"
            )
        
        # Identifica tipo de evento pelo campo 'mode'
        mode = row.get('mode', '').strip().upper()
        
        # Evento EXEC (mode: FULL, LOCK, FREE)
        if mode in ['FULL', 'LOCK', 'FREE']:
            return self._parse_exec_event(row, timestamp, task_id, mode)
        
        # Evento MOVE (mode: DOWN, PICK, BACK, EDIT)
        elif mode in ['DOWN', 'PICK', 'BACK', 'EDIT']:
            return self._parse_move_event(row, timestamp, task_id, mode)
        
        # Evento SELF (mode: SELF)
        elif mode == 'SELF':
            return self._parse_self_event(row, timestamp, task_id)
        
        else:
            raise ParseError(
                line_num=0,
                raw_line=str(row),
                reason=f"Unknown mode: '{mode}'"
            )
    
    def _parse_exec_event(
        self, 
        row: dict, 
        timestamp: datetime, 
        task_id: str, 
        mode: str
    ) -> ExecEvent:
        """Parseia evento de execução."""
        try:
            # rate é opcional para FREE, obrigatório para FULL/LOCK
            rate_str = row.get('rate', '').strip()
            rate = int(rate_str) if rate_str else None
            
            size_str = row.get('size', '').strip()
            size = int(size_str) if size_str else 0
            
            error = row.get('error', 'NONE').strip().upper()
            if not error:
                error = 'NONE'
            
            return ExecEvent(
                timestamp=timestamp,
                task_id=task_id,
                mode=mode,
                rate=rate,
                size=size,
                error=error
            )
        except (ValueError, KeyError) as e:
            raise ParseError(
                line_num=0,
                raw_line=str(row),
                reason=f"Invalid ExecEvent fields: {e}"
            )
    
    def _parse_move_event(
        self, 
        row: dict, 
        timestamp: datetime, 
        task_id: str, 
        mode: str
    ) -> MoveEvent:
        """Parseia evento de navegação."""
        try:
            return MoveEvent.from_mode(
                mode=mode,
                timestamp=timestamp,
                task_id=task_id
            )
        except ValueError as e:
            raise ParseError(
                line_num=0,
                raw_line=str(row),
                reason=f"Invalid MoveEvent: {e}"
            )
    
    def _parse_self_event(
        self, 
        row: dict, 
        timestamp: datetime, 
        task_id: str
    ) -> SelfEvent:
        """Parseia evento de autoavaliação."""
        try:
            rate_str = row.get('rate', '').strip()
            rate = int(rate_str) if rate_str else None
            
            autonomy_str = row.get('autonomy', '').strip()
            autonomy = int(autonomy_str) if autonomy_str else 0
            
            # Campos de ajuda (strings descrevendo ajuda)
            help_human = row.get('help_human', '').strip() or None
            help_iagen = row.get('help_iagen', '').strip() or None
            help_guide = row.get('help_guide', '').strip() or None
            help_other = row.get('help_other', '').strip() or None
            
            study_str = row.get('study', '').strip()
            study_minutes = int(study_str) if study_str else 0
            
            return SelfEvent(
                timestamp=timestamp,
                task_id=task_id,
                rate=rate,
                autonomy=autonomy,
                help_human=help_human,
                help_iagen=help_iagen,
                help_guide=help_guide,
                help_other=help_other,
                study_minutes=study_minutes
            )
        except (ValueError, KeyError) as e:
            raise ParseError(
                line_num=0,
                raw_line=str(row),
                reason=f"Invalid SelfEvent fields: {e}"
            )
    
    def _parse_bool(self, value: str) -> bool:
        """
        Converte string CSV para booleano.
        
        Aceita: '1', 'true', 'yes', 't' (case-insensitive) → True
                Qualquer outro valor → False
        """
        return value.strip().lower() in ['1', 'true', 'yes', 't']
