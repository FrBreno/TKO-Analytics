"""
Conversor de eventos TKO para formato XES (eXtensible Event Stream).

Este módulo converte eventos TKO armazenados no banco de dados SQLite
para o formato XES padrão usado pelo PM4Py e outras ferramentas de Process Mining.
"""

import structlog
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET
from xml.dom import minidom

logger = structlog.get_logger()


class TKOToXESConverter:
    """Converte eventos TKO do banco de dados para formato XES."""
    
    def __init__(self, db_path: str):
        """
        Inicializa o conversor.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        logger.info("[TKOToXESConverter.__init__] - converter_initialized", db_path=db_path)
    
    def _create_xes_header(self) -> ET.Element:
        """Cria cabeçalho XES com metadados."""
        log = ET.Element('log')
        log.set('xes.version', '2.0')
        log.set('xes.features', 'nested-attributes')
        log.set('xmlns', 'http://www.xes-standard.org/')
        
        # Metadados
        ET.SubElement(log, 'string', key='concept:name', value='TKO Event Log')
        ET.SubElement(log, 'string', key='lifecycle:model', value='standard')
        ET.SubElement(log, 'string', key='source', value='TKO Analytics')
        ET.SubElement(log, 'string', key='description', value='Educational process mining log from TKO telemetry')
        
        # Classificadores globais
        classifier = ET.SubElement(log, 'classifier')
        classifier.set('name', 'Activity')
        classifier.set('keys', 'concept:name')
        
        classifier_resource = ET.SubElement(log, 'classifier')
        classifier_resource.set('name', 'Activity+Resource')
        classifier_resource.set('keys', 'concept:name org:resource')
        
        # Extensões globais
        ET.SubElement(log, 'extension', name='Concept', prefix='concept', uri='http://www.xes-standard.org/concept.xesext')
        ET.SubElement(log, 'extension', name='Time', prefix='time', uri='http://www.xes-standard.org/time.xesext')
        ET.SubElement(log, 'extension', name='Organizational', prefix='org', uri='http://www.xes-standard.org/org.xesext')
        ET.SubElement(log, 'extension', name='Lifecycle', prefix='lifecycle', uri='http://www.xes-standard.org/lifecycle.xesext')
        
        return log
    
    def _add_event_to_trace(self, trace: ET.Element, event_data: Dict) -> None:
        """
        Adiciona um evento ao trace XES.
        
        Args:
            trace: Elemento XML do trace
            event_data: Dicionário com dados do evento do banco
        """
        event = ET.SubElement(trace, 'event')
        
        # Mapeamento do event_type
        activity_name = self._map_event_type_to_activity(event_data['event_type'])
        ET.SubElement(event, 'string', key='concept:name', value=activity_name)
        
        # Timestamp
        timestamp = event_data['timestamp']
        if isinstance(timestamp, str):
            # Converte ISO format
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        else:
            dt = timestamp
        
        ET.SubElement(event, 'date', key='time:timestamp', value=dt.isoformat())
        
        # Pseudonimização do recurso (student_hash)
        if event_data.get('student_hash'):
            ET.SubElement(event, 'string', key='org:resource', value=event_data['student_hash'])
        
        # Transição do ciclo de vida
        ET.SubElement(event, 'string', key='lifecycle:transition', value='complete')
        
        # Atributos específicos do TKO
        if event_data.get('task_id'):
            ET.SubElement(event, 'string', key='tko:task', value=event_data['task_id'])
        
        if event_data.get('size') is not None:
            ET.SubElement(event, 'int', key='tko:result_size', value=str(event_data['size']))
        
        if event_data.get('result') is not None:
            result_str = 'pass' if event_data['result'] else 'fail'
            ET.SubElement(event, 'string', key='tko:result', value=result_str)
        
        # Metadata adicional (se disponível)
        if event_data.get('metadata'):
            try:
                import json
                metadata = json.loads(event_data['metadata']) if isinstance(event_data['metadata'], str) else event_data['metadata']
                
                # Adiciona metadados relevantes
                if isinstance(metadata, dict):
                    for key, value in metadata.items():
                        if key in ['estimated_time', 'autonomy', 'help_received', 'difficulty']:
                            ET.SubElement(event, 'string', key=f'tko:{key}', value=str(value))
            except Exception as e:
                logger.warning("[TKOToXESConverter._add_event_to_trace] - metadata_parse_failed", error=str(e))
    
    def _map_event_type_to_activity(self, event_type: str) -> str:
        """
        Mapeia tipos de evento TKO para nomes de atividades do processo.
        
        Args:
            event_type: Tipo do evento TKO (ExecEvent, MoveEvent, SelfEvent, etc.)
            
        Returns:
            Nome da atividade no processo
        """
        # Normaliza variações (ExecEvent, exec, EXEC)
        event_type_lower = event_type.lower().replace('event', '')
        
        mapping = {
            'exec': 'Execute Code',
            'move': 'Edit Code',
            'self': 'Self-Assessment',
            'down': 'Download Task',
            'open': 'Open Task',
            'diff': 'View Diff',
            'help': 'Request Help',
            'submit': 'Submit Solution',
        }
        
        return mapping.get(event_type_lower, event_type)
    
    def convert_student(
        self,
        student_id: str,
        task_id: Optional[str] = None,
        output_path: Optional[str] = None
    ) -> str:
        """
        Converte eventos de um estudante para XES.
        
        Args:
            student_id: ID do estudante (pseudônimo)
            task_id: ID da tarefa (opcional, se None converte todas)
            output_path: Caminho para salvar XES (opcional)
            
        Returns:
            Caminho do arquivo XES gerado
            
        Raises:
            Exception: Se conversão falhar
        """
        logger.info(
            "[TKOToXESConverter.convert_student] - converting_student",
            student_id=student_id,
            task_id=task_id
        )
        
        # Cria cabeçalho XES
        log = self._create_xes_header()
        
        # Conecta ao banco
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Query para buscar eventos
            if task_id:
                query = """
                SELECT * FROM events
                WHERE student_hash = ? AND task_id = ?
                ORDER BY timestamp ASC
                """
                cursor.execute(query, (student_id, task_id))
            else:
                query = """
                SELECT * FROM events
                WHERE student_hash = ?
                ORDER BY timestamp ASC
                """
                cursor.execute(query, (student_id,))
            
            events = cursor.fetchall()
            
            if not events:
                logger.warning(
                    "[TKOToXESConverter.convert_student] - no_events_found",
                    student_id=student_id,
                    task_id=task_id
                )
                raise Exception(f"Nenhum evento encontrado para student={student_id}, task={task_id}")
            
            # Agrupa eventos por task (cada task = 1 trace)
            tasks_events = {}
            for event in events:
                tid = event['task_id']
                if tid not in tasks_events:
                    tasks_events[tid] = []
                tasks_events[tid].append(dict(event))
            
            # Cria traces
            for tid, task_events in tasks_events.items():
                trace = ET.SubElement(log, 'trace')
                
                # Atributos do trace
                ET.SubElement(trace, 'string', key='concept:name', value=f"{student_id}_{tid}")
                ET.SubElement(trace, 'string', key='tko:student_hash', value=student_id)
                ET.SubElement(trace, 'string', key='tko:task_id', value=tid)
                
                # Adiciona eventos ao trace
                for event_data in task_events:
                    self._add_event_to_trace(trace, event_data)
            
            # Determina caminho de saída
            if not output_path:
                output_dir = Path(self.db_path).parent / 'xes_exports'
                output_dir.mkdir(exist_ok=True)
                
                if task_id:
                    output_path = str(output_dir / f"{student_id}_{task_id}.xes")
                else:
                    output_path = str(output_dir / f"{student_id}_all_tasks.xes")
            
            # Salva XML formatado
            xml_str = ET.tostring(log, encoding='unicode')
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent='  ')
            
            # Remove linhas vazias extras
            pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)
            
            logger.info(
                "[TKOToXESConverter.convert_student] - conversion_successful",
                student_id=student_id,
                task_id=task_id,
                traces=len(tasks_events),
                events=len(events),
                output=output_path
            )
            
            return output_path
            
        finally:
            conn.close()
    
    def convert_all_students(
        self,
        output_path: Optional[str] = None
    ) -> str:
        """
        Converte eventos de TODOS os estudantes para um único arquivo XES.
        
        Args:
            output_path: Caminho para salvar XES (opcional)
            
        Returns:
            Caminho do arquivo XES gerado
        """
        logger.info("[TKOToXESConverter.convert_all_students] - converting_all")
        
        # Cria cabeçalho XES
        log = self._create_xes_header()
        
        # Conecta ao banco
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Query para buscar todos os eventos ordenados
            query = """
            SELECT * FROM events
            ORDER BY student_hash, task_id, timestamp ASC
            """
            cursor.execute(query)
            events = cursor.fetchall()
            
            if not events:
                logger.warning("[TKOToXESConverter.convert_all_students] - no_events_found")
                raise Exception("Nenhum evento encontrado no banco")
            
            # Agrupa eventos por (student, task) - cada combinação = 1 trace
            traces_events = {}
            for event in events:
                case_id = f"{event['student_hash']}_{event['task_id']}"
                if case_id not in traces_events:
                    traces_events[case_id] = {
                        'student_hash': event['student_hash'],
                        'task_id': event['task_id'],
                        'events': []
                    }
                traces_events[case_id]['events'].append(dict(event))
            
            # Cria traces
            for case_id, trace_data in traces_events.items():
                trace = ET.SubElement(log, 'trace')
                
                # Atributos do trace
                ET.SubElement(trace, 'string', key='concept:name', value=case_id)
                ET.SubElement(trace, 'string', key='tko:student_hash', value=trace_data['student_hash'])
                ET.SubElement(trace, 'string', key='tko:task_id', value=trace_data['task_id'])
                
                # Adiciona eventos ao trace
                for event_data in trace_data['events']:
                    self._add_event_to_trace(trace, event_data)
            
            # Determina caminho de saída
            if not output_path:
                output_dir = Path(self.db_path).parent / 'xes_exports'
                output_dir.mkdir(exist_ok=True)
                output_path = str(output_dir / 'tko_all_students.xes')
            
            # Salva XML formatado
            xml_str = ET.tostring(log, encoding='unicode')
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent='  ')
            
            # Remove linhas vazias extras
            pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)
            
            logger.info(
                "[TKOToXESConverter.convert_all_students] - conversion_successful",
                traces=len(traces_events),
                total_events=len(events),
                students=len(set(t['student_hash'] for t in traces_events.values())),
                tasks=len(set(t['task_id'] for t in traces_events.values())),
                output=output_path
            )
            
            return output_path
            
        finally:
            conn.close()
    
    def convert_task(
        self,
        task_id: str,
        output_path: Optional[str] = None
    ) -> str:
        """
        Converte eventos de UMA tarefa (todos os estudantes) para XES.
        
        Args:
            task_id: ID da tarefa
            output_path: Caminho para salvar XES (opcional)
            
        Returns:
            Caminho do arquivo XES gerado
        """
        logger.info("[TKOToXESConverter.convert_task] - converting_task", task_id=task_id)
        
        # Cria cabeçalho XES
        log = self._create_xes_header()
        
        # Conecta ao banco
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Query para buscar eventos da tarefa
            query = """
            SELECT * FROM events
            WHERE task_id = ?
            ORDER BY student_hash, timestamp ASC
            """
            cursor.execute(query, (task_id,))
            events = cursor.fetchall()
            
            if not events:
                logger.warning("[TKOToXESConverter.convert_task] - no_events_found", task_id=task_id)
                raise Exception(f"Nenhum evento encontrado para task={task_id}")
            
            # Agrupa eventos por student (cada student = 1 trace)
            students_events = {}
            for event in events:
                sid = event['student_hash']
                if sid not in students_events:
                    students_events[sid] = []
                students_events[sid].append(dict(event))
            
            # Cria traces
            for sid, student_events in students_events.items():
                trace = ET.SubElement(log, 'trace')
                
                # Atributos do trace
                ET.SubElement(trace, 'string', key='concept:name', value=f"{sid}_{task_id}")
                ET.SubElement(trace, 'string', key='tko:student_hash', value=sid)
                ET.SubElement(trace, 'string', key='tko:task_id', value=task_id)
                
                # Adiciona eventos ao trace
                for event_data in student_events:
                    self._add_event_to_trace(trace, event_data)
            
            # Determina caminho de saída
            if not output_path:
                output_dir = Path(self.db_path).parent / 'xes_exports'
                output_dir.mkdir(exist_ok=True)
                output_path = str(output_dir / f"task_{task_id}.xes")
            
            # Salva XML formatado
            xml_str = ET.tostring(log, encoding='unicode')
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent='  ')
            
            # Remove linhas vazias extras
            pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)
            
            logger.info(
                "[TKOToXESConverter.convert_task] - conversion_successful",
                task_id=task_id,
                traces=len(students_events),
                events=len(events),
                students=len(students_events),
                output=output_path
            )
            
            return output_path
            
        finally:
            conn.close()
