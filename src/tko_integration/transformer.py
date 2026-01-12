"""
Módulo Transformer para conversão de dados TKO para formato CSV TKO-Analytics.

Responsável por:
- Pseudonimização de IDs de estudantes
- Conversão do formato de eventos TKO para CSV
- Remoção de prefixos de tarefas (ex.: poo@)
- Normalização de valores de campos
"""

import csv
import hashlib
from pathlib import Path
from typing import List, Dict, Any

from .parser import TrackingParser
from .scanner import ClassroomScan, StudentRepo
from .parser import LogParser, TKOLogEvent, ValueNormalizer


class TKOTransformer:
    """
    Transforma dados TKO para formato CSV TKO-Analytics.
    """
    
    def __init__(self, student_id_salt: str):
        """
        Inicializa transformer com salt para pseudonimização.
        
        Args:
            student_id_salt: Salt secreto para hash de IDs de estudantes
        """
        self.student_id_salt = student_id_salt
    
    def pseudonymize_student_id(self, username: str) -> str:
        """
        Cria ID pseudônimo a partir do username usando SHA256.
        
        Args:
            username: Username original (ex.: "F0NSII")
            
        Returns:
            Hash de 8 caracteres (ex.: "a1b2c3d4")
        """
        data = f"{username}{self.student_id_salt}"
        hash_obj = hashlib.sha256(data.encode('utf-8'))
        return hash_obj.hexdigest()[:8]
    
    def normalize_task_key(self, task_key: str) -> str:
        """
        Normaliza chave de tarefa removendo prefixos.
        
        Exemplos:
            poo@toalha -> toalha
            toalha -> toalha
        """
        # Remove prefixos comuns
        prefixes = ['poo@', 'fup@', 'ed@', 'repo@']
        for prefix in prefixes:
            if task_key.startswith(prefix):
                return task_key[len(prefix):]
        return task_key
    
    def event_to_csv_row(self, event: TKOLogEvent, student_hash: str) -> Dict[str, Any]:
        """
        Converte TKOLogEvent para dicionário de linha CSV.
        
        Args:
            event: Evento de log TKO
            student_hash: ID pseudonimizado do estudante
            
        Returns:
            Dict com valores das colunas CSV
        """
        # Normaliza chave da tarefa
        task_id = self.normalize_task_key(event.task_key)
        
        # Normaliza valores
        human = ValueNormalizer.normalize_human(event.human)
        iagen = ValueNormalizer.normalize_ai(event.iagen)
        guide = ValueNormalizer.normalize_guide(event.guide)
        
        return {
            'timestamp': event.timestamp.isoformat(),
            'student_id': student_hash,
            'task': task_id,
            'event_type': event.event_type,
            'mode': event.mode or '',
            'rate': event.rate if event.rate is not None else '',
            'size': event.size if event.size is not None else '',
            'human': human or '',
            'iagen': iagen or '',
            'guide': guide or '',
            'other': event.other or '',
            'alone': event.alone if event.alone is not None else '',
            'study': event.study if event.study is not None else '',
        }
    
    def transform_scan_to_csv(
        self, 
        scan: ClassroomScan, 
        output_path: Path,
        include_tracking: bool = False
    ) -> int:
        """
        Transforma varredura completa para arquivo CSV.
        
        Args:
            scan: Resultado do ClassroomScan
            output_path: Caminho para arquivo CSV de saída
            include_tracking: Se deve incluir dados de rastreamento (padrão: False)
            
        Returns:
            Número de eventos escritos
        """
        total_events = 0
        all_rows = []
        
        # Processar cada estudante
        for turma in scan.turmas:
            for block in turma.blocks:
                for student in block.students:
                    if not student.valid:
                        continue
                    
                    student_hash = self.pseudonymize_student_id(student.username)
                    log_dir = student.tko_dir / 'log'
                    if log_dir.exists():
                        events = LogParser.parse_all_logs(log_dir)
                        
                        for event in events:
                            row = self.event_to_csv_row(event, student_hash)
                            all_rows.append(row)
                            total_events += 1
                    
                    if include_tracking:
                        self._add_tracking_rows(
                            student, 
                            student_hash, 
                            all_rows
                        )
        
        # Ordenar por timestamp
        all_rows.sort(key=lambda r: r['timestamp'])
        # Escrever CSV
        fieldnames = [
            'timestamp', 'student_id', 'task', 'event_type', 'mode', 
            'rate', 'size', 'human', 'iagen', 'guide', 'other', 'alone', 'study'
        ]
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        
        return total_events
    
    def _add_tracking_rows(
        self, 
        student: StudentRepo, 
        student_hash: str, 
        all_rows: List[Dict[str, Any]]
    ) -> None:
        """
        Adiciona dados de rastreamento como eventos sintéticos.
        
        Cria eventos CODE_SNAPSHOT a partir dos dados de rastreamento.
        """
        track_dir = student.tko_dir / 'track'
        if not track_dir.exists():
            return
        
        # Processar cada diretório de tarefa
        for task_dir in track_dir.iterdir():
            if not task_dir.is_dir():
                continue
            
            task_key = task_dir.name
            tracking_data = TrackingParser.parse_task_tracking(task_dir, task_key)
            
            # Adicionar eventos de snapshot do histórico
            for snapshot in tracking_data['history']:
                row = {
                    'timestamp': snapshot.timestamp.isoformat(),
                    'student_id': student_hash,
                    'task': self.normalize_task_key(task_key),
                    'event_type': 'CODE_SNAPSHOT',
                    'mode': '',
                    'rate': '',
                    'size': snapshot.size,
                    'human': '',
                    'iagen': '',
                    'guide': '',
                    'other': '',
                    'alone': '',
                    'study': '',
                }
                all_rows.append(row)
    
    def transform_single_student(
        self,
        student: StudentRepo,
        output_path: Path
    ) -> int:
        """
        Transforma dados de um único estudante para CSV.
        
        Útil para testes ou processamento de estudantes individuais.
        """
        if not student.valid:
            return 0
        
        student_hash = self.pseudonymize_student_id(student.username)
        all_rows = []
        
        # Parsear logs
        log_dir = student.tko_dir / 'log'
        if log_dir.exists():
            events = LogParser.parse_all_logs(log_dir)
            
            for event in events:
                row = self.event_to_csv_row(event, student_hash)
                all_rows.append(row)
        
        # Ordenar e escrever
        all_rows.sort(key=lambda r: r['timestamp'])
        fieldnames = [
            'timestamp', 'student_id', 'task', 'event_type', 'mode', 
            'rate', 'size', 'human', 'iagen', 'guide', 'other', 'alone', 'study'
        ]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        
        return len(all_rows)
