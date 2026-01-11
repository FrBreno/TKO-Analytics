"""
XES Exporter - Exporta eventos para formato XES (eXtensible Event Stream).

O formato XES é o padrão IEEE para Process Mining e é compatível com
ferramentas como PM4Py, ProM, Disco, entre outras.

Referência: IEEE Standard for eXtensible Event Stream (XES) - https://xes-standard.org/
"""

import sqlite3
import structlog
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET
from xml.dom import minidom

logger = structlog.get_logger()


class XESExportError(Exception):
    """Erro durante exportação XES."""
    pass


class XESExporter:
    """
    Exporta eventos TKO para formato XES.
    
    O formato XES organiza eventos em:
    - Log: Coleção de traces (casos)
    - Trace: Sequência de eventos de um caso (estudante em uma tarefa)
    - Event: Evento individual com timestamp e atributos
    
    Atributos globais suportados:
    - concept:name (nome da atividade)
    - time:timestamp (timestamp do evento)
    - org:resource (estudante anonimizado)
    - lifecycle:transition (sempre "complete")
    
    Atributos customizados:
    - tko:task_id
    - tko:event_type (ExecEvent, MoveEvent, SelfEvent)
    - tko:metadata (JSON com dados específicos)
    """
    
    XES_VERSION = "1849.2016"
    XES_FEATURES = "nested-attributes"
    
    def __init__(self):
        """Inicializa exportador XES."""
        pass
    
    def export_from_db(
        self,
        db_path: str,
        output_path: str,
        case_id: Optional[str] = None,
        task_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Exporta eventos do banco SQLite para arquivo XES.
        
        Args:
            db_path: Caminho do banco SQLite
            output_path: Caminho do arquivo XES de saída
            case_id: Filtro opcional por case_id
            task_id: Filtro opcional por task_id
        
        Returns:
            Estatísticas da exportação:
            {
                'traces': número de traces,
                'events': número de eventos,
                'cases': casos únicos
            }
        
        Raises:
            XESExportError: Se houver erro na exportação
        """
        logger.info(
            "[XESExporter.export_from_db] - xes_export_started",
            db_path=db_path,
            output_path=output_path,
            case_id=case_id,
            task_id=task_id
        )
        
        events = self._load_events_from_db(db_path, case_id, task_id)
        if not events:
            raise XESExportError("No events found to export")
        traces = self._group_events_into_traces(events)
        xes_root = self._create_xes_structure(traces)
        
        self._save_xes_file(xes_root, output_path)
        
        stats = {
            'traces': len(traces),
            'events': len(events),
            'cases': len(set(e['case_id'] for e in events))
        }
        
        logger.info(
            "[XESExporter.export_from_db] - xes_export_completed",
            output_path=output_path,
            **stats
        )
        
        return stats
    
    def _load_events_from_db(
        self,
        db_path: str,
        case_id: Optional[str],
        task_id: Optional[str]
    ) -> List[Dict]:
        """
        Carrega eventos do banco SQLite.
        
        Args:
            db_path: Caminho do banco
            case_id: Filtro opcional por case_id
            task_id: Filtro opcional por task_id
        
        Returns:
            Lista de eventos como dicionários
        """
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    id,
                    case_id,
                    student_hash,
                    task_id,
                    activity,
                    event_type,
                    timestamp,
                    duration_seconds,
                    session_id,
                    metadata
                FROM events
                WHERE 1=1
            """
            params = []
            
            if case_id:
                query += " AND case_id = ?"
                params.append(case_id)
            
            if task_id:
                query += " AND task_id = ?"
                params.append(task_id)
            
            query += " ORDER BY timestamp ASC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in rows]
        
        except sqlite3.Error as e:
            raise XESExportError(f"Failed to load events from database: {e}")
    
    def _group_events_into_traces(self, events: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Agrupa eventos em traces (case_id + task_id).
        
        Um trace representa a sequência de eventos de um estudante
        trabalhando em uma tarefa específica.
        
        Args:
            events: Lista de eventos
        
        Returns:
            Dicionário {trace_id: [eventos]}
        """
        traces = {}
        
        for event in events:
            # Trace ID = case_id + task_id
            trace_id = f"{event['case_id']}_{event['task_id']}"
            
            if trace_id not in traces:
                traces[trace_id] = []
            
            traces[trace_id].append(event)
        
        return traces
    
    def _create_xes_structure(self, traces: Dict[str, List[Dict]]) -> ET.Element:
        """
        Cria estrutura XML XES.
        
        Args:
            traces: Dicionário de traces com eventos
        
        Returns:
            Elemento raiz do XML XES
        """
        # Root element
        log = ET.Element('log')
        log.set('xes.version', self.XES_VERSION)
        log.set('xes.features', self.XES_FEATURES)
        log.set('xmlns', 'http://www.xes-standard.org/')
        
        self._add_extensions(log)
        self._add_classifiers(log)
        self._add_global_attributes(log)
        
        # Traces
        for trace_id, events in traces.items():
            trace_elem = self._create_trace(trace_id, events)
            log.append(trace_elem)
        
        return log
    
    def _add_extensions(self, log: ET.Element) -> None:
        """Adiciona extensões XES padrão."""
        extensions = [
            ('Concept', 'http://www.xes-standard.org/concept.xesext', 'concept'),
            ('Time', 'http://www.xes-standard.org/time.xesext', 'time'),
            ('Organizational', 'http://www.xes-standard.org/org.xesext', 'org'),
            ('Lifecycle', 'http://www.xes-standard.org/lifecycle.xesext', 'lifecycle'),
        ]
        
        for name, uri, prefix in extensions:
            ext = ET.SubElement(log, 'extension')
            ext.set('name', name)
            ext.set('uri', uri)
            ext.set('prefix', prefix)
    
    def _add_classifiers(self, log: ET.Element) -> None:
        """Adiciona classificadores de atividades."""
        # Activity classifier
        classifier = ET.SubElement(log, 'classifier')
        classifier.set('name', 'Activity')
        classifier.set('keys', 'concept:name')
        
        # Resource classifier
        classifier = ET.SubElement(log, 'classifier')
        classifier.set('name', 'Resource')
        classifier.set('keys', 'org:resource')
    
    def _add_global_attributes(self, log: ET.Element) -> None:
        """Adiciona atributos globais."""
        # Global event attributes
        global_event = ET.SubElement(log, 'global')
        global_event.set('scope', 'event')
        
        # concept:name
        string_attr = ET.SubElement(global_event, 'string')
        string_attr.set('key', 'concept:name')
        string_attr.set('value', '__INVALID__')
        
        # time:timestamp
        date_attr = ET.SubElement(global_event, 'date')
        date_attr.set('key', 'time:timestamp')
        date_attr.set('value', '1970-01-01T00:00:00.000+00:00')
        
        # org:resource
        string_attr = ET.SubElement(global_event, 'string')
        string_attr.set('key', 'org:resource')
        string_attr.set('value', '__INVALID__')
        
        # lifecycle:transition
        string_attr = ET.SubElement(global_event, 'string')
        string_attr.set('key', 'lifecycle:transition')
        string_attr.set('value', 'complete')
    
    def _create_trace(self, trace_id: str, events: List[Dict]) -> ET.Element:
        """
        Cria elemento trace com seus eventos.
        
        Args:
            trace_id: ID do trace
            events: Lista de eventos do trace
        
        Returns:
            Elemento trace
        """
        trace = ET.Element('trace')
        
        # Trace attributes
        string_attr = ET.SubElement(trace, 'string')
        string_attr.set('key', 'concept:name')
        string_attr.set('value', trace_id)
        
        # Adiciona case_id como atributo
        if events:
            case_attr = ET.SubElement(trace, 'string')
            case_attr.set('key', 'tko:case_id')
            case_attr.set('value', events[0]['case_id'])
            
            task_attr = ET.SubElement(trace, 'string')
            task_attr.set('key', 'tko:task_id')
            task_attr.set('value', events[0]['task_id'])
            
            student_attr = ET.SubElement(trace, 'string')
            student_attr.set('key', 'tko:student_hash')
            student_attr.set('value', events[0]['student_hash'])
        
        # Events
        for event_data in events:
            event_elem = self._create_event(event_data)
            trace.append(event_elem)
        
        return trace
    
    def _create_event(self, event_data: Dict) -> ET.Element:
        """
        Cria elemento event com atributos.
        
        Args:
            event_data: Dados do evento
        
        Returns:
            Elemento event
        """
        event = ET.Element('event')
        
        # concept:name (activity)
        string_attr = ET.SubElement(event, 'string')
        string_attr.set('key', 'concept:name')
        string_attr.set('value', event_data['activity'])
        
        # time:timestamp
        date_attr = ET.SubElement(event, 'date')
        date_attr.set('key', 'time:timestamp')
        # Converte para formato XES (ISO 8601 com milissegundos)
        timestamp = datetime.fromisoformat(event_data['timestamp'])
        date_attr.set('value', timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+00:00')
        
        # org:resource (student_hash)
        string_attr = ET.SubElement(event, 'string')
        string_attr.set('key', 'org:resource')
        string_attr.set('value', event_data['student_hash'])
        
        # lifecycle:transition
        string_attr = ET.SubElement(event, 'string')
        string_attr.set('key', 'lifecycle:transition')
        string_attr.set('value', 'complete')
        
        # Custom attributes
        string_attr = ET.SubElement(event, 'string')
        string_attr.set('key', 'tko:event_type')
        string_attr.set('value', event_data['event_type'])
        
        string_attr = ET.SubElement(event, 'string')
        string_attr.set('key', 'tko:event_id')
        string_attr.set('value', event_data['id'])
        
        if event_data['session_id']:
            string_attr = ET.SubElement(event, 'string')
            string_attr.set('key', 'tko:session_id')
            string_attr.set('value', event_data['session_id'])
        
        if event_data['duration_seconds'] is not None:
            int_attr = ET.SubElement(event, 'int')
            int_attr.set('key', 'tko:duration_seconds')
            int_attr.set('value', str(event_data['duration_seconds']))
        
        # Metadata (como string JSON)
        if event_data['metadata']:
            string_attr = ET.SubElement(event, 'string')
            string_attr.set('key', 'tko:metadata')
            string_attr.set('value', event_data['metadata'])
        
        return event
    
    def _save_xes_file(self, root: ET.Element, output_path: str) -> None:
        """
        Salva XML XES em arquivo formatado.
        
        Args:
            root: Elemento raiz do XML
            output_path: Caminho do arquivo de saída
        """
        try:
            # Converte para string XML
            xml_str = ET.tostring(root, encoding='unicode')
            
            # Pretty print
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent='  ', encoding='UTF-8')
            
            # Salva arquivo
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_bytes(pretty_xml)
            
        except Exception as e:
            raise XESExportError(f"Failed to save XES file: {e}")


def export_to_xes(
    db_path: str,
    output_path: str,
    case_id: Optional[str] = None,
    task_id: Optional[str] = None
) -> Dict[str, int]:
    """
    Função helper para exportar eventos para XES.
    
    Args:
        db_path: Caminho do banco SQLite
        output_path: Caminho do arquivo XES de saída
        case_id: Filtro opcional por case_id
        task_id: Filtro opcional por task_id
    
    Returns:
        Estatísticas da exportação
    """
    exporter = XESExporter()
    return exporter.export_from_db(db_path, output_path, case_id, task_id)
