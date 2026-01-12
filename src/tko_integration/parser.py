"""
Módulo parser para arquivos de log TKO, repository.yaml e dados de rastreamento.

Manipula:
- Arquivos de log (.tko/log/*.log) com eventos MOVE, EXEC, SELF
- Tarefas do repository.yaml com dados de autoavaliação
- Dados de rastreamento (draft.py.json) com snapshots de código
"""

import yaml
import json
import structlog
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

logger = structlog.get_logger()

@dataclass
class TKOLogEvent:
    """Representa um evento dos arquivos de log TKO."""
    timestamp: datetime
    event_type: str  # MOVE, EXEC, SELF
    version: int
    task_key: str
    mode: Optional[str] = None  # DOWN, PICK, BACK, LOCK, FREE, FULL
    rate: Optional[int] = None  # 0-100
    size: Optional[int] = None  # Linhas de código
    # Campos de evento SELF
    human: Optional[str] = None
    iagen: Optional[str] = None
    guide: Optional[str] = None
    other: Optional[str] = None
    alone: Optional[int] = None  # 0-10
    study: Optional[int] = None  # Minutos


@dataclass
class TKOTaskData:
    """Dados de uma tarefa no repository.yaml."""
    task_key: str
    rate: Optional[int] = None
    human: Optional[str] = None
    iagen: Optional[str] = None
    guide: Optional[str] = None
    other: Optional[str] = None
    alone: Optional[int] = None
    study: Optional[int] = None


@dataclass
class CodeSnapshot:
    """Representa um snapshot de código dos dados de rastreamento."""
    timestamp: datetime
    task_key: str
    code: str
    size: int  # Linhas de código
    diff_from_previous: Optional[str] = None


class LogParser:
    """Parser para arquivos de log TKO (.tko/log/*.log)."""
    
    @staticmethod
    def parse_log_line(line: str) -> Optional[TKOLogEvent]:
        """
        Analisa uma única linha de arquivo de log TKO.
        
        Formato:
            YYYY-MM-DD HH:MM:SS, TYPE, v:N, k:KEY, [campos adicionais]
        
        Exemplos:
            2025-09-18 02:44:25, MOVE, v:1, k:toalha, mode:DOWN
            2025-09-18 02:44:34, EXEC, v:1, k:toalha, mode:LOCK, rate:100, size:0
            2025-09-16 19:53:28, SELF, v:1, k:toalha, rate:100, guide:sena, alone:9, study:15
        """
        line = line.strip()
        if not line:
            return None
        
        try:
            parts = [p.strip() for p in line.split(',')]            
            if len(parts) < 4:
                return None
            timestamp_str = parts[0]
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            event_type = parts[1]
            version_part = parts[2]
            task_part = parts[3]
            
            if not version_part.startswith('v:') or not task_part.startswith('k:'):
                return None
            
            version = int(version_part.split(':')[1])
            task_key = task_part.split(':', 1)[1]
            
            # Analisar campos adicionais
            fields = {}
            for part in parts[4:]:
                if ':' in part:
                    key, value = part.split(':', 1)
                    fields[key] = value
            
            # Converter campos numéricos
            rate = int(fields['rate']) if 'rate' in fields else None
            size = int(fields['size']) if 'size' in fields else None
            alone = int(fields['alone']) if 'alone' in fields else None
            study = int(fields['study']) if 'study' in fields else None
            
            return TKOLogEvent(
                timestamp=timestamp,
                event_type=event_type,
                version=version,
                task_key=task_key,
                mode=fields.get('mode'),
                rate=rate,
                size=size,
                human=fields.get('human'),
                iagen=fields.get('iagen'),
                guide=fields.get('guide'),
                other=fields.get('other'),
                alone=alone,
                study=study,
            )
        except (ValueError, IndexError):
            logger.warn(f"[LogParser.parse_log_line] - Linha malformada ignorada: {line}")
            return None
    
    @staticmethod
    def parse_log_file(file_path: Path) -> List[TKOLogEvent]:
        """Analisa um arquivo de log completo e retorna lista de eventos."""
        events = []
        
        if not file_path.exists():
            return events
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    event = LogParser.parse_log_line(line)
                    if event:
                        events.append(event)
        except Exception as e:
            logger.warn(f"[LogParser.parse_log_file] - Falha ao analisar {file_path}: {e}")
        
        return events
    
    @staticmethod
    def parse_all_logs(log_dir: Path) -> List[TKOLogEvent]:
        """Analisa todos os arquivos de log em um diretório."""
        all_events = []
  
        if not log_dir.exists():
            return all_events
        
        for log_file in sorted(log_dir.glob('*.log')):
            events = LogParser.parse_log_file(log_file)
            all_events.extend(events)
        
        all_events.sort(key=lambda e: e.timestamp)        
        return all_events


class RepositoryParser:
    """Parser para arquivos repository.yaml."""
    
    @staticmethod
    def parse_task_value(value_str: str) -> Dict[str, Any]:
        """
        Analisa string de valor de tarefa do repository.yaml.
        
        Formato: '{rate:100, human:SIM, alone:6, study:120}'
        
        Retorna dict com pares chave-valor analisados.
        """
        value_str = value_str.strip("'{} ")
        
        if not value_str:
            return {}
        
        pairs = value_str.split(',') 
        result = {}

        for pair in pairs:
            pair = pair.strip()
            if ':' in pair:
                key, value = pair.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                try:
                    result[key] = int(value)
                except ValueError:
                    result[key] = value
        return result
    
    @staticmethod
    def parse_repository_yaml(file_path: Path) -> Dict[str, TKOTaskData]:
        """
        Analisa repository.yaml e retorna dict de tarefas.
        
        Retorna:
            Dict mapeando task_key para TKOTaskData
        """
        tasks = {}
        
        if not file_path.exists():
            return tasks
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data or 'tasks' not in data:
                return tasks
            
            for task_key, task_value_str in data['tasks'].items():
                # Manipular formatos string e dict
                if isinstance(task_value_str, str):
                    parsed = RepositoryParser.parse_task_value(task_value_str)
                elif isinstance(task_value_str, dict):
                    parsed = task_value_str
                else:
                    continue
                
                tasks[task_key] = TKOTaskData(
                    task_key=task_key,
                    rate=parsed.get('rate'),
                    human=parsed.get('human'),
                    iagen=parsed.get('iagen'),
                    guide=parsed.get('guide'),
                    other=parsed.get('other'),
                    alone=parsed.get('alone'),
                    study=parsed.get('study'),
                )
        except Exception as e:
            logger.warn(f"[RepositoryParser.parse_repository_yaml] - Falha ao analisar {file_path}: {e}")
        
        return tasks


class TrackingParser:
    """Parser para dados de rastreamento TKO (.tko/track/)."""
    
    @staticmethod
    def parse_draft_json(file_path: Path, task_key: str) -> Optional[CodeSnapshot]:
        """
        Analisa arquivo draft.py.json para extrair snapshot de código atual.
        
        Args:
            file_path: Caminho para draft.py.json
            task_key: Identificador da tarefa
            
        Retorna:
            CodeSnapshot com código atual ou None se a análise falhar
        """
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            code = data.get('content', '')  
            timestamp = datetime.fromtimestamp(file_path.stat().st_mtime)
            size = len(code.split('\n'))
            
            return CodeSnapshot(
                timestamp=timestamp,
                task_key=task_key,
                code=code,
                size=size,
            )
        except Exception as e:
            logger.warn(f"[TrackingParser.parse_draft_json] - Falha ao analisar {file_path}: {e}")
            return None
    
    @staticmethod
    def parse_track_csv(file_path: Path, task_key: str) -> List[CodeSnapshot]:
        """
        Analisa track.csv para obter histórico de snapshots de código.
        
        Retorna lista de objetos CodeSnapshot ordenados por timestamp.
        """
        snapshots = []
        
        if not file_path.exists():
            return snapshots
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                
                if not line:
                    continue
                parts = line.split(',')
                
                if len(parts) >= 1:
                    try:
                        timestamp = datetime.fromtimestamp(float(parts[0]))
                        
                        snapshot = CodeSnapshot(
                            timestamp=timestamp,
                            task_key=task_key,
                            code="",
                            size=0,
                        )
                        snapshots.append(snapshot)
                    except (ValueError, IndexError):
                        continue
        except Exception as e:
            logger.warn(f"[TrackingParser.parse_track_csv] - Falha ao analisar {file_path}: {e}")
        
        return snapshots
    
    @staticmethod
    def parse_task_tracking(track_dir: Path, task_key: str) -> Dict[str, Any]:
        """
        Analisa todos os dados de rastreamento de uma tarefa.
        
        Retorna:
            Dict com 'draft' (snapshot atual) e 'history' (lista de snapshots)
        """
        result = {
            'draft': None,
            'history': [],
        }
        
        if not track_dir.exists():
            return result
        
        # Analisar draft.py.json (ou draft.js.json, etc.)
        for draft_file in track_dir.glob('draft.*'):
            if draft_file.suffix == '.json':
                result['draft'] = TrackingParser.parse_draft_json(draft_file, task_key)
                break
        
        # Analisar track.csv
        track_csv = track_dir / 'track.csv'
        if track_csv.exists():
            result['history'] = TrackingParser.parse_track_csv(track_csv, task_key)
        
        return result


class ValueNormalizer:
    """Normaliza valores inconsistentes dos logs TKO."""
    
    HUMAN_HELP_MAP = {
        'sim': 'yes',
        'SIM': 'yes',
        'nao': 'none',
        'NAO': 'none',
        '_amigo': 'friend',
        'sim_amigo': 'friend',
        'sim_amigo_monitor': 'friend_monitor',
        'Monitor': 'monitor',
        'monitor': 'monitor',
    }
    
    AI_USAGE_MAP = {
        'sim': 'yes',
        'SIM': 'yes',
        'nao': 'none',
        'NAO': 'none',
        'copilot': 'github_copilot',
        '_gpt': 'chatgpt',
        '_copilot': 'github_copilot',
        'copilot_e_gpt': 'copilot_chatgpt',
        'gemini': 'google_gemini',
        'autocomplete_apenas_copilot': 'copilot_autocomplete',
    }
    
    GUIDE_MAP = {
        'sena': 'professor',
        'readme': 'readme',
        'README': 'readme',
        'sena_e_readme': 'professor_readme',
        'professor': 'professor',
        'sim': 'yes',
        'SIM': 'yes',
        '_video': 'video',
        'video': 'video',
        'Aula': 'class',
        'aula': 'class',
    }
    
    @staticmethod
    def normalize_human(value: Optional[str]) -> Optional[str]:
        """Normaliza campo de ajuda humana."""
        if not value:
            return None
        return ValueNormalizer.HUMAN_HELP_MAP.get(value, value)
    
    @staticmethod
    def normalize_ai(value: Optional[str]) -> Optional[str]:
        """Normaliza campo de uso de IA."""
        if not value:
            return None
        return ValueNormalizer.AI_USAGE_MAP.get(value, value)
    
    @staticmethod
    def normalize_guide(value: Optional[str]) -> Optional[str]:
        """Normaliza campo de material guia."""
        if not value:
            return None
        return ValueNormalizer.GUIDE_MAP.get(value, value)
