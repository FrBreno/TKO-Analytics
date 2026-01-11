"""
Testes para XESExporter.
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

from tko_analytics.models.events import ExecEvent, MoveEvent, SelfEvent
from tko_analytics.etl.loader import SQLiteLoader
from tko_analytics.etl.init_db import init_database
from tko_analytics.exporters import XESExporter, XESExportError, export_to_xes


@pytest.fixture
def temp_db(tmp_path):
    """Cria banco temporário com eventos para testes."""
    db_path = tmp_path / "test_xes.db"
    init_database(str(db_path))
    
    # Cria eventos de exemplo
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    events = [
        MoveEvent(timestamp=base_time, task_id="calc", action="PICK"),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=5),
            task_id="calc",
            mode="FULL",
            rate=50,
            size=80
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=10),
            task_id="calc",
            mode="FULL",
            rate=100,
            size=90
        ),
        SelfEvent(
            timestamp=base_time + timedelta(minutes=15),
            task_id="calc",
            rate=100,
            autonomy=9
        ),
    ]
    
    # Carrega no banco
    loader = SQLiteLoader(str(db_path))
    loader.load_events(
        events,
        student_id="student_test",
        case_id="case_test",
        session_id="session_test"
    )
    
    return str(db_path)


@pytest.fixture
def temp_output(tmp_path):
    """Caminho para arquivo XES de saída."""
    return str(tmp_path / "output.xes")


@pytest.fixture
def exporter():
    """Cria instância do exportador."""
    return XESExporter()


class TestXESExporterInitialization:
    """Testes de inicialização."""
    
    def test_create_exporter(self):
        """Testa criação do exportador."""
        exporter = XESExporter()
        assert exporter is not None
        assert exporter.XES_VERSION == "1849.2016"
        assert exporter.XES_FEATURES == "nested-attributes"


class TestXESExportFromDB:
    """Testes de exportação do banco."""
    
    def test_export_all_events(self, exporter, temp_db, temp_output):
        """Testa exportação de todos os eventos."""
        stats = exporter.export_from_db(temp_db, temp_output)
        
        assert stats['events'] == 4
        assert stats['traces'] == 1  # Todos na mesma tarefa
        assert stats['cases'] == 1
        
        # Verifica que arquivo foi criado
        assert Path(temp_output).exists()
    
    def test_export_with_case_filter(self, exporter, temp_db, temp_output):
        """Testa exportação com filtro por case_id."""
        stats = exporter.export_from_db(
            temp_db,
            temp_output,
            case_id="case_test"
        )
        
        assert stats['events'] == 4
        assert Path(temp_output).exists()
    
    def test_export_empty_result(self, exporter, temp_db, temp_output):
        """Testa erro quando não há eventos."""
        with pytest.raises(XESExportError, match="No events found"):
            exporter.export_from_db(
                temp_db,
                temp_output,
                case_id="nonexistent"
            )
    
    def test_export_creates_directory(self, exporter, temp_db, tmp_path):
        """Testa que diretório é criado se não existir."""
        nested_path = tmp_path / "subdir" / "output.xes"
        
        stats = exporter.export_from_db(temp_db, str(nested_path))
        
        assert nested_path.exists()
        assert stats['events'] == 4


class TestXESStructure:
    """Testes da estrutura XML XES."""
    
    # Namespace XES
    XES_NS = {'xes': 'http://www.xes-standard.org/'}
    
    def test_xes_root_attributes(self, exporter, temp_db, temp_output):
        """Testa atributos do elemento raiz."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        # Remove namespace do tag para comparação
        assert root.tag.split('}')[-1] == 'log'
        assert root.get('xes.version') == "1849.2016"
        assert root.get('xes.features') == "nested-attributes"
        # Verifica que o namespace está presente no tag
        assert 'http://www.xes-standard.org/' in root.tag
    
    def test_xes_has_extensions(self, exporter, temp_db, temp_output):
        """Testa presença de extensões XES."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        # Busca com namespace
        extensions = root.findall('xes:extension', self.XES_NS)
        assert len(extensions) == 4
        
        prefixes = {ext.get('prefix') for ext in extensions}
        assert 'concept' in prefixes
        assert 'time' in prefixes
        assert 'org' in prefixes
        assert 'lifecycle' in prefixes
    
    def test_xes_has_classifiers(self, exporter, temp_db, temp_output):
        """Testa presença de classificadores."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        classifiers = root.findall('xes:classifier', self.XES_NS)
        assert len(classifiers) >= 2
        
        names = {c.get('name') for c in classifiers}
        assert 'Activity' in names
        assert 'Resource' in names
    
    def test_xes_has_global_attributes(self, exporter, temp_db, temp_output):
        """Testa atributos globais."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        global_elems = root.findall('xes:global[@scope="event"]', self.XES_NS)
        assert len(global_elems) >= 1
        
        # Verifica atributos globais de evento
        global_event = global_elems[0]
        attrs = {child.get('key') for child in global_event}
        assert 'concept:name' in attrs
        assert 'time:timestamp' in attrs
        assert 'org:resource' in attrs
        assert 'lifecycle:transition' in attrs


class TestXESTraces:
    """Testes de traces."""
    
    XES_NS = {'xes': 'http://www.xes-standard.org/'}
    
    def test_trace_created(self, exporter, temp_db, temp_output):
        """Testa criação de trace."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        traces = root.findall('xes:trace', self.XES_NS)
        assert len(traces) == 1
    
    def test_trace_has_name(self, exporter, temp_db, temp_output):
        """Testa que trace tem concept:name."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        trace = root.find('xes:trace', self.XES_NS)
        name_elem = trace.find("xes:string[@key='concept:name']", self.XES_NS)
        
        assert name_elem is not None
        assert 'case_test_calc' in name_elem.get('value')
    
    def test_trace_has_custom_attributes(self, exporter, temp_db, temp_output):
        """Testa atributos customizados do trace."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        trace = root.find('xes:trace', self.XES_NS)
        
        case_elem = trace.find("xes:string[@key='tko:case_id']", self.XES_NS)
        assert case_elem is not None
        assert case_elem.get('value') == 'case_test'
        
        task_elem = trace.find("xes:string[@key='tko:task_id']", self.XES_NS)
        assert task_elem is not None
        assert task_elem.get('value') == 'calc'
        
        student_elem = trace.find("xes:string[@key='tko:student_hash']", self.XES_NS)
        assert student_elem is not None


class TestXESEvents:
    """Testes de eventos."""
    
    XES_NS = {'xes': 'http://www.xes-standard.org/'}
    
    def test_events_created(self, exporter, temp_db, temp_output):
        """Testa criação de eventos."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        trace = root.find('xes:trace', self.XES_NS)
        events = trace.findall('xes:event', self.XES_NS)
        
        assert len(events) == 4
    
    def test_event_has_required_attributes(self, exporter, temp_db, temp_output):
        """Testa atributos obrigatórios do evento."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        trace = root.find('xes:trace', self.XES_NS)
        event = trace.find('xes:event', self.XES_NS)
        
        # concept:name
        name_elem = event.find("xes:string[@key='concept:name']", self.XES_NS)
        assert name_elem is not None
        
        # time:timestamp
        time_elem = event.find("xes:date[@key='time:timestamp']", self.XES_NS)
        assert time_elem is not None
        timestamp_value = time_elem.get('value')
        assert 'T' in timestamp_value  # ISO 8601 format
        assert '+00:00' in timestamp_value  # Timezone
        
        # org:resource
        resource_elem = event.find("xes:string[@key='org:resource']", self.XES_NS)
        assert resource_elem is not None
        
        # lifecycle:transition
        lifecycle_elem = event.find("xes:string[@key='lifecycle:transition']", self.XES_NS)
        assert lifecycle_elem is not None
        assert lifecycle_elem.get('value') == 'complete'
    
    def test_event_has_custom_attributes(self, exporter, temp_db, temp_output):
        """Testa atributos customizados do evento."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        trace = root.find('xes:trace', self.XES_NS)
        event = trace.find('xes:event', self.XES_NS)
        
        # tko:event_type
        type_elem = event.find("xes:string[@key='tko:event_type']", self.XES_NS)
        assert type_elem is not None
        assert type_elem.get('value') in ['ExecEvent', 'MoveEvent', 'SelfEvent']
        
        # tko:event_id
        id_elem = event.find("xes:string[@key='tko:event_id']", self.XES_NS)
        assert id_elem is not None
        
        # tko:session_id
        session_elem = event.find("xes:string[@key='tko:session_id']", self.XES_NS)
        assert session_elem is not None
        assert session_elem.get('value') == 'session_test'
    
    def test_event_metadata_as_json(self, exporter, temp_db, temp_output):
        """Testa que metadata é exportado como JSON string."""
        exporter.export_from_db(temp_db, temp_output)
        
        tree = ET.parse(temp_output)
        root = tree.getroot()
        
        trace = root.find('xes:trace', self.XES_NS)
        events = trace.findall('xes:event', self.XES_NS)
        
        # Procura evento com metadata (ExecEvent)
        for event in events:
            metadata_elem = event.find("xes:string[@key='tko:metadata']", self.XES_NS)
            if metadata_elem is not None:
                import json
                # Verifica que é JSON válido
                metadata_str = metadata_elem.get('value')
                metadata = json.loads(metadata_str)
                assert isinstance(metadata, dict)
                break
        else:
            pytest.fail("No event with metadata found")


class TestHelperFunction:
    """Testes da função helper."""
    
    def test_export_to_xes_helper(self, temp_db, temp_output):
        """Testa função helper export_to_xes."""
        stats = export_to_xes(temp_db, temp_output)
        
        assert stats['events'] == 4
        assert stats['traces'] == 1
        assert Path(temp_output).exists()
    
    def test_export_to_xes_with_filters(self, temp_db, temp_output):
        """Testa função helper com filtros."""
        stats = export_to_xes(
            temp_db,
            temp_output,
            case_id="case_test",
            task_id="calc"
        )
        
        assert stats['events'] == 4
        assert Path(temp_output).exists()


class TestMultipleTraces:
    """Testes com múltiplos traces."""
    
    XES_NS = {'xes': 'http://www.xes-standard.org/'}
    
    def test_multiple_tasks_create_multiple_traces(self, tmp_path):
        """Testa que tarefas diferentes criam traces diferentes."""
        db_path = tmp_path / "multi_trace.db"
        init_database(str(db_path))
        
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Eventos para duas tarefas diferentes
        events = [
            MoveEvent(timestamp=base_time, task_id="task1", action="PICK"),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=5),
                task_id="task1",
                mode="FULL",
                rate=100,
                size=80
            ),
            MoveEvent(
                timestamp=base_time + timedelta(minutes=10),
                task_id="task2",
                action="PICK"
            ),
            ExecEvent(
                timestamp=base_time + timedelta(minutes=15),
                task_id="task2",
                mode="FULL",
                rate=100,
                size=90
            ),
        ]
        
        loader = SQLiteLoader(str(db_path))
        loader.load_events(events, "student1", "case1")
        
        # Exporta
        output_path = tmp_path / "multi.xes"
        exporter = XESExporter()
        stats = exporter.export_from_db(str(db_path), str(output_path))
        
        # Deve criar 2 traces (task1 e task2)
        assert stats['traces'] == 2
        assert stats['events'] == 4
        
        # Verifica XML
        tree = ET.parse(str(output_path))
        root = tree.getroot()
        traces = root.findall('xes:trace', self.XES_NS)
        assert len(traces) == 2


class TestXMLValidity:
    """Testes de validade do XML."""
    
    def test_xml_is_well_formed(self, exporter, temp_db, temp_output):
        """Testa que XML gerado é bem formado."""
        exporter.export_from_db(temp_db, temp_output)
        
        # Se conseguir parsear, está bem formado
        tree = ET.parse(temp_output)
        assert tree is not None
    
    def test_xml_has_utf8_encoding(self, exporter, temp_db, temp_output):
        """Testa que arquivo usa encoding UTF-8."""
        exporter.export_from_db(temp_db, temp_output)
        
        content = Path(temp_output).read_bytes()
        
        # Verifica declaração XML
        assert b'<?xml' in content
        assert b'UTF-8' in content or b'utf-8' in content
