"""
Process Discovery for TKO Analytics.

Este módulo facilita a descoberta de processos a partir de eventos TKO,
integrando o conversor XES e o analisador PM4Py.
"""

import structlog
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from .tko_to_xes import TKOToXESConverter
from .analyzer import ProcessAnalyzer, ProcessAnalysisResult

logger = structlog.get_logger()


@dataclass
class ProcessDiscoveryResult:
    """Resultado da descoberta de processo com métricas TKO."""
    
    # Análise PM4Py
    pm4py_analysis: ProcessAnalysisResult
    
    # Informações do XES gerado
    xes_path: str
    num_students: int
    num_tasks: int
    
    # Estatísticas TKO
    avg_events_per_trace: float
    most_common_activity: str
    most_common_activity_count: int
    
    def __str__(self) -> str:
        """Representação string do resultado."""
        lines = [
            f"TKO Process Discovery Results",
            f"",
            f"Dataset Statistics:",
            f"   • Students: {self.num_students}",
            f"   • Tasks: {self.num_tasks}",
            f"   • Traces: {self.pm4py_analysis.num_traces}",
            f"   • Events: {self.pm4py_analysis.num_events}",
            f"   • Avg events/trace: {self.avg_events_per_trace:.1f}",
            f"",
            f"Most Common Activity: {self.most_common_activity} ({self.most_common_activity_count} occurrences)",
            f"",
            f"--- PM4Py Analysis ---",
            str(self.pm4py_analysis),
            f"",
            f"XES file saved at: {self.xes_path}"
        ]
        return "\n".join(lines)


class ProcessDiscovery:
    """Facilitador para descoberta de processos a partir de eventos TKO."""
    
    def __init__(self, db_path: str):
        """
        Inicializa o Process Discovery.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        self.converter = TKOToXESConverter(db_path)
        self.analyzer = ProcessAnalyzer()
        
        logger.info("[ProcessDiscovery.__init__] - process_discovery_initialized", db_path=db_path)
    
    def discover_all_students(
        self,
        xes_output_path: Optional[str] = None,
        model_output_path: Optional[str] = None,
        compute_conformance: bool = True
    ) -> ProcessDiscoveryResult:
        """
        Descobre processo para TODOS os estudantes (dataset completo).
        
        Fluxo:
        1. Converte eventos TKO → XES
        2. Carrega XES no PM4Py
        3. Descobre modelo de processo (Inductive Miner)
        4. Calcula conformance (fitness, precision)
        5. Analisa variantes
        6. Salva visualização do modelo
        
        Args:
            xes_output_path: Caminho para salvar XES (opcional)
            model_output_path: Caminho para salvar visualização (opcional)
            compute_conformance: Se deve calcular conformance (True recomendado)
            
        Returns:
            ProcessDiscoveryResult com todas as análises
        """
        logger.info("[ProcessDiscovery.discover_all_students] - starting_discovery")
        
        # 1. Converte TKO → XES
        xes_path = self.converter.convert_all_students(output_path=xes_output_path)
        
        # 2. Analisa com PM4Py
        pm4py_result = self.analyzer.analyze(
            xes_path=xes_path,
            discover_model=True,
            compute_conformance=compute_conformance,
            top_variants=10
        )
        
        # 3. Salva visualização do modelo (se Graphviz disponível)
        if model_output_path:
            try:
                self.analyzer.save_model_visualization(model_output_path, format='png')
            except Exception as e:
                logger.warning(
                    "[ProcessDiscovery.discover_all_students] - visualization_skipped",
                    reason=str(e)
                )
        else:
            # Salva no mesmo diretório do XES
            xes_dir = Path(xes_path).parent
            default_model_path = xes_dir / 'process_model_all_students.png'
            try:
                self.analyzer.save_model_visualization(str(default_model_path), format='png')
            except Exception as e:
                logger.warning(
                    "[ProcessDiscovery.discover_all_students] - visualization_skipped",
                    reason=str(e)
                )
        
        # 4. Estatísticas adicionais TKO
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Conta estudantes e tarefas únicas
        cursor.execute("SELECT COUNT(DISTINCT student_hash) FROM events")
        num_students = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT task_id) FROM events")
        num_tasks = cursor.fetchone()[0]
        
        # Atividade mais comum
        cursor.execute("""
            SELECT event_type, COUNT(*) as cnt
            FROM events
            GROUP BY event_type
            ORDER BY cnt DESC
            LIMIT 1
        """)
        most_common = cursor.fetchone()
        most_common_activity = most_common[0] if most_common else "N/A"
        most_common_count = most_common[1] if most_common else 0
        
        conn.close()
        
        # Calcula média de eventos por trace
        avg_events = pm4py_result.num_events / pm4py_result.num_traces if pm4py_result.num_traces > 0 else 0
        
        # 5. Monta resultado
        result = ProcessDiscoveryResult(
            pm4py_analysis=pm4py_result,
            xes_path=xes_path,
            num_students=num_students,
            num_tasks=num_tasks,
            avg_events_per_trace=avg_events,
            most_common_activity=most_common_activity,
            most_common_activity_count=most_common_count
        )
        
        logger.info(
            "[ProcessDiscovery.discover_all_students] - discovery_completed",
            students=num_students,
            tasks=num_tasks,
            traces=pm4py_result.num_traces,
            events=pm4py_result.num_events
        )
        
        return result
    
    def discover_task(
        self,
        task_id: str,
        xes_output_path: Optional[str] = None,
        model_output_path: Optional[str] = None,
        compute_conformance: bool = True
    ) -> ProcessDiscoveryResult:
        """
        Descobre processo para UMA tarefa específica (todos os estudantes).
        
        Útil para comparar como diferentes estudantes resolvem a mesma tarefa.
        
        Args:
            task_id: ID da tarefa
            xes_output_path: Caminho para salvar XES (opcional)
            model_output_path: Caminho para salvar visualização (opcional)
            compute_conformance: Se deve calcular conformance
            
        Returns:
            ProcessDiscoveryResult com análises da tarefa
        """
        logger.info("[ProcessDiscovery.discover_task] - starting_discovery", task_id=task_id)
        
        # 1. Converte TKO → XES
        xes_path = self.converter.convert_task(task_id, output_path=xes_output_path)
        
        # 2. Analisa com PM4Py
        pm4py_result = self.analyzer.analyze(
            xes_path=xes_path,
            discover_model=True,
            compute_conformance=compute_conformance,
            top_variants=10
        )
        
        # 3. Salva visualização do modelo (se Graphviz disponível)
        if model_output_path:
            try:
                self.analyzer.save_model_visualization(model_output_path, format='png')
            except Exception as e:
                logger.warning(
                    "[ProcessDiscovery.discover_task] - visualization_skipped",
                    task_id=task_id,
                    reason=str(e)
                )
        else:
            xes_dir = Path(xes_path).parent
            default_model_path = xes_dir / f'process_model_task_{task_id}.png'
            try:
                self.analyzer.save_model_visualization(str(default_model_path), format='png')
            except Exception as e:
                logger.warning(
                    "[ProcessDiscovery.discover_task] - visualization_skipped",
                    task_id=task_id,
                    reason=str(e)
                )
        
        # 4. Estatísticas adicionais
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT student_hash) FROM events WHERE task_id = ?", (task_id,))
        num_students = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT event_type, COUNT(*) as cnt
            FROM events
            WHERE task_id = ?
            GROUP BY event_type
            ORDER BY cnt DESC
            LIMIT 1
        """, (task_id,))
        most_common = cursor.fetchone()
        most_common_activity = most_common[0] if most_common else "N/A"
        most_common_count = most_common[1] if most_common else 0
        
        conn.close()
        
        avg_events = pm4py_result.num_events / pm4py_result.num_traces if pm4py_result.num_traces > 0 else 0
        
        result = ProcessDiscoveryResult(
            pm4py_analysis=pm4py_result,
            xes_path=xes_path,
            num_students=num_students,
            num_tasks=1,
            avg_events_per_trace=avg_events,
            most_common_activity=most_common_activity,
            most_common_activity_count=most_common_count
        )
        
        logger.info(
            "[ProcessDiscovery.discover_task] - discovery_completed",
            task_id=task_id,
            students=num_students,
            traces=pm4py_result.num_traces,
            events=pm4py_result.num_events
        )
        
        return result
    
    def discover_student(
        self,
        student_id: str,
        task_id: Optional[str] = None,
        xes_output_path: Optional[str] = None,
        model_output_path: Optional[str] = None,
        compute_conformance: bool = True
    ) -> ProcessDiscoveryResult:
        """
        Descobre processo para UM estudante específico.
        
        Args:
            student_id: ID do estudante (pseudônimo)
            task_id: ID da tarefa (opcional, se None analisa todas)
            xes_output_path: Caminho para salvar XES (opcional)
            model_output_path: Caminho para salvar visualização (opcional)
            compute_conformance: Se deve calcular conformance
            
        Returns:
            ProcessDiscoveryResult com análises do estudante
        """
        logger.info(
            "[ProcessDiscovery.discover_student] - starting_discovery",
            student_id=student_id,
            task_id=task_id
        )
        
        # 1. Converte TKO → XES
        xes_path = self.converter.convert_student(
            student_id=student_id,
            task_id=task_id,
            output_path=xes_output_path
        )
        
        # 2. Analisa com PM4Py
        pm4py_result = self.analyzer.analyze(
            xes_path=xes_path,
            discover_model=True,
            compute_conformance=compute_conformance,
            top_variants=10
        )
        
        # 3. Salva visualização do modelo (se Graphviz disponível)
        if model_output_path:
            try:
                self.analyzer.save_model_visualization(model_output_path, format='png')
            except Exception as e:
                logger.warning(
                    "[ProcessDiscovery.discover_student] - visualization_skipped",
                    student_id=student_id,
                    task_id=task_id,
                    reason=str(e)
                )
        else:
            xes_dir = Path(xes_path).parent
            filename = f'process_model_student_{student_id}'
            if task_id:
                filename += f'_task_{task_id}'
            filename += '.png'
            default_model_path = xes_dir / filename
            try:
                self.analyzer.save_model_visualization(str(default_model_path), format='png')
            except Exception as e:
                logger.warning(
                    "[ProcessDiscovery.discover_student] - visualization_skipped",
                    student_id=student_id,
                    task_id=task_id,
                    reason=str(e)
                )
        
        # 4. Estatísticas adicionais
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if task_id:
            cursor.execute(
                "SELECT COUNT(DISTINCT task_id) FROM events WHERE student_hash = ? AND task_id = ?",
                (student_id, task_id)
            )
            num_tasks = 1
            
            cursor.execute("""
                SELECT event_type, COUNT(*) as cnt
                FROM events
                WHERE student_hash = ? AND task_id = ?
                GROUP BY event_type
                ORDER BY cnt DESC
                LIMIT 1
            """, (student_id, task_id))
        else:
            cursor.execute(
                "SELECT COUNT(DISTINCT task_id) FROM events WHERE student_hash = ?",
                (student_id,)
            )
            num_tasks = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT event_type, COUNT(*) as cnt
                FROM events
                WHERE student_hash = ?
                GROUP BY event_type
                ORDER BY cnt DESC
                LIMIT 1
            """, (student_id,))
        
        most_common = cursor.fetchone()
        most_common_activity = most_common[0] if most_common else "N/A"
        most_common_count = most_common[1] if most_common else 0
        
        conn.close()
        
        avg_events = pm4py_result.num_events / pm4py_result.num_traces if pm4py_result.num_traces > 0 else 0
        
        result = ProcessDiscoveryResult(
            pm4py_analysis=pm4py_result,
            xes_path=xes_path,
            num_students=1,
            num_tasks=num_tasks,
            avg_events_per_trace=avg_events,
            most_common_activity=most_common_activity,
            most_common_activity_count=most_common_count
        )
        
        logger.info(
            "[ProcessDiscovery.discover_student] - discovery_completed",
            student_id=student_id,
            task_id=task_id,
            tasks=num_tasks,
            traces=pm4py_result.num_traces,
            events=pm4py_result.num_events
        )
        
        return result
    
    def compare_students(
        self,
        student_ids: List[str],
        task_id: str,
        output_dir: Optional[str] = None
    ) -> Dict[str, ProcessDiscoveryResult]:
        """
        Compara processos de múltiplos estudantes na MESMA tarefa.
        
        Útil para identificar diferentes estratégias de resolução.
        
        Args:
            student_ids: Lista de IDs de estudantes
            task_id: ID da tarefa comum
            output_dir: Diretório para salvar visualizações (opcional)
            
        Returns:
            Dicionário {student_id: ProcessDiscoveryResult}
        """
        logger.info(
            "[ProcessDiscovery.compare_students] - starting_comparison",
            num_students=len(student_ids),
            task_id=task_id
        )
        
        results = {}
        
        for student_id in student_ids:
            # Define caminhos de saída
            xes_path = None
            model_path = None
            
            if output_dir:
                out_dir = Path(output_dir)
                out_dir.mkdir(parents=True, exist_ok=True)
                xes_path = str(out_dir / f"student_{student_id}_task_{task_id}.xes")
                model_path = str(out_dir / f"model_student_{student_id}_task_{task_id}.png")
            
            # Descobre processo para o estudante
            result = self.discover_student(
                student_id=student_id,
                task_id=task_id,
                xes_output_path=xes_path,
                model_output_path=model_path,
                compute_conformance=True
            )
            
            results[student_id] = result
        
        logger.info(
            "[ProcessDiscovery.compare_students] - comparison_completed",
            num_students=len(student_ids),
            task_id=task_id
        )
        
        return results
