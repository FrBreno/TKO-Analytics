"""
ProcessModelGenerator: Geração de modelo de processo usando Inductive Miner.

Este módulo implementa a geração de um modelo de processo global
a partir dos eventos do dataset MODEL, usando PM4Py.
"""

import sqlite3
import structlog
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

import pm4py
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.log.obj import EventLog, Trace, Event

logger = structlog.get_logger()


class ModelGenerationError(Exception):
    """Erro durante geração do modelo de processo."""
    pass


class ProcessModelGenerator:
    """
    Gerador de modelo de processo usando Inductive Miner.
    
    Responsável por:
    1. Extrair eventos do dataset MODEL do banco
    2. Converter para EventLog (XES-like)
    3. Aplicar Inductive Miner
    4. Retornar Petri Net e marcações
    """
    
    def __init__(self, db_path: str):
        """
        Inicializa o gerador de modelos.
        
        Args:
            db_path: Caminho do banco de dados SQLite
            
        Raises:
            ModelGenerationError: Se banco não existe
        """
        self.db_path = Path(db_path)
        
        if not self.db_path.exists():
            raise ModelGenerationError(f"Database not found: {self.db_path}")
        
        logger.info("[ProcessModelGenerator.__init__] - Initialized", db_path=str(self.db_path))
    
    def generate_model(
        self,
        noise_threshold: float = 0.2,
        task_id: Optional[str] = None
    ) -> Tuple[PetriNet, Marking, Marking]:
        """
        Gera modelo de processo a partir dos eventos MODEL.
        
        Args:
            noise_threshold: Threshold para filtrar ruído (0.0 a 1.0)
                           0.0 = modelo mais específico
                           1.0 = modelo mais generalizado
            task_id: Se fornecido, gera modelo apenas para essa tarefa.
                    Se None, gera modelo global com todas as tarefas.
        
        Returns:
            Tupla (net, initial_marking, final_marking)
            
        Raises:
            ModelGenerationError: Se não há eventos MODEL suficientes
        """
        logger.info("[ProcessModelGenerator.generate_model] - Starting model generation",
                   noise_threshold=noise_threshold,
                   task_id=task_id or "ALL_TASKS")
        
        # 1. Extrai eventos MODEL do banco (com filtro opcional)
        model_events = self._fetch_model_events(task_id=task_id)
        
        if len(model_events) == 0:
            raise ModelGenerationError("No MODEL events found in database")
        
        logger.info("[ProcessModelGenerator.generate_model] - Fetched MODEL events",
                   total_events=len(model_events))
        
        # 2. Converte para EventLog (PM4Py)
        event_log = self._events_to_log(model_events)
        
        logger.info("[ProcessModelGenerator.generate_model] - Converted to EventLog",
                   traces=len(event_log),
                   events=sum(len(trace) for trace in event_log))
        
        # 3. Aplica Inductive Miner
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(
            event_log,
            noise_threshold=noise_threshold
        )
        
        logger.info("[ProcessModelGenerator.generate_model] - Model generated successfully",
                   places=len(net.places),
                   transitions=len(net.transitions),
                   arcs=len(net.arcs))
        
        return net, initial_marking, final_marking
    
    def _fetch_model_events(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Busca eventos da tabela model_events, opcionalmente filtrados por tarefa.
        
        Args:
            task_id: Se fornecido, filtra apenas eventos dessa tarefa
        
        Returns:
            Lista de eventos como dicionários
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if task_id:
            # Modelo específico para uma tarefa
            cursor.execute("""
                SELECT 
                    id,
                    case_id,
                    student_hash,
                    task_id,
                    activity,
                    event_type,
                    timestamp,
                    metadata
                FROM model_events
                WHERE task_id = ?
                ORDER BY case_id, timestamp ASC
            """, (task_id,))
            logger.info("[_fetch_model_events] - Fetching for specific task", task_id=task_id)
        else:
            # Modelo global (todas as tarefas)
            cursor.execute("""
                SELECT 
                    id,
                    case_id,
                    student_hash,
                    task_id,
                    activity,
                    event_type,
                    timestamp,
                    metadata
                FROM model_events
                ORDER BY case_id, timestamp ASC
            """)
            logger.info("[_fetch_model_events] - Fetching all tasks (global model)")
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def _events_to_log(self, events: List[Dict[str, Any]]) -> EventLog:
        """
        Converte lista de eventos em PM4Py EventLog.
        
        Agrupa eventos por case_id (estudante + tarefa).
        
        Args:
            events: Lista de eventos do banco
            
        Returns:
            EventLog formatado para PM4Py
        """
        # Agrupa por case_id
        traces_dict: Dict[str, List[Dict[str, Any]]] = {}
        
        for event in events:
            case_id = event["case_id"]
            if case_id not in traces_dict:
                traces_dict[case_id] = []
            traces_dict[case_id].append(event)
        
        # Converte para EventLog
        event_log = EventLog()
        
        for case_id, case_events in traces_dict.items():
            trace = Trace()
            trace.attributes["concept:name"] = case_id
            
            # Extrai student_hash e task_id do primeiro evento
            if case_events:
                trace.attributes["student_hash"] = case_events[0]["student_hash"]
                trace.attributes["task_id"] = case_events[0]["task_id"]
            
            for evt in case_events:
                pm4py_event = Event()
                pm4py_event["concept:name"] = evt["activity"]
                pm4py_event["time:timestamp"] = datetime.fromisoformat(evt["timestamp"])
                pm4py_event["case:id"] = case_id
                pm4py_event["event_type"] = evt["event_type"]
                
                trace.append(pm4py_event)
            
            event_log.append(trace)
        
        return event_log
    
    def get_model_statistics(self, task_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna estatísticas dos eventos MODEL no banco.
        
        Args:
            task_id: Se fornecido, estatísticas apenas para essa tarefa
        
        Returns:
            Dicionário com estatísticas:
            - total_events: Total de eventos MODEL
            - total_traces: Total de traces (case_id únicos)
            - unique_activities: Atividades únicas
            - date_range: Intervalo de datas
            - student_count: Número de estudantes únicos
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        where_clause = "WHERE task_id = ?" if task_id else ""
        params = (task_id,) if task_id else ()
        
        # Total de eventos
        cursor.execute(f"SELECT COUNT(*) FROM model_events {where_clause}", params)
        total_events = cursor.fetchone()[0]
        
        # Total de traces
        cursor.execute(f"SELECT COUNT(DISTINCT case_id) FROM model_events {where_clause}", params)
        total_traces = cursor.fetchone()[0]
        
        # Estudantes únicos
        cursor.execute(f"SELECT COUNT(DISTINCT student_hash) FROM model_events {where_clause}", params)
        student_count = cursor.fetchone()[0]
        
        # Atividades únicas
        cursor.execute(f"SELECT DISTINCT activity FROM model_events {where_clause}", params)
        activities = [row[0] for row in cursor.fetchall()]
        
        # Intervalo de datas
        cursor.execute(f"""
            SELECT MIN(timestamp), MAX(timestamp) 
            FROM model_events {where_clause}
        """, params)
        date_range = cursor.fetchone()
        
        conn.close()
        
        return {
            "total_events": total_events,
            "total_traces": total_traces,
            "student_count": student_count,
            "unique_activities": activities,
            "activity_count": len(activities),
            "date_range": {
                "start": date_range[0] if date_range[0] else None,
                "end": date_range[1] if date_range[1] else None
            }
        }
    
    def get_dfg(self, task_id: Optional[str] = None) -> Tuple[Dict[Tuple[str, str], int], Dict[str, int], Dict[str, int]]:
        """
        Gera Directly-Follows Graph (DFG) dos eventos MODEL.
        
        Args:
            task_id: Se fornecido, DFG apenas para essa tarefa
        
        Returns:
            Tupla (dfg, start_activities, end_activities)
            - dfg: {(activity_a, activity_b): frequency}
            - start_activities: {activity: count}
            - end_activities: {activity: count}
        """
        model_events = self._fetch_model_events(task_id=task_id)
        event_log = self._events_to_log(model_events)
        
        # pm4py.discover_dfg retorna tupla (dfg, start_activities, end_activities)
        dfg_tuple = pm4py.discover_dfg(event_log)
        
        # Desempacotar a tupla retornada pelo pm4py
        if isinstance(dfg_tuple, tuple) and len(dfg_tuple) == 3:
            dfg, start_activities, end_activities = dfg_tuple
        else:
            # Fallback: se retornar apenas dfg (versões antigas do pm4py)
            dfg = dfg_tuple
            start_activities = pm4py.get_start_activities(event_log)
            end_activities = pm4py.get_end_activities(event_log)
        
        return dfg, start_activities, end_activities
    
    def get_available_tasks(self) -> List[Dict[str, Any]]:
        """
        Retorna lista de tarefas disponíveis nos dados MODEL.
        
        Returns:
            Lista de dicts com task_id, event_count, student_count
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                task_id,
                COUNT(*) as event_count,
                COUNT(DISTINCT student_hash) as student_count
            FROM model_events
            GROUP BY task_id
            ORDER BY event_count DESC
        """)
        
        tasks = []
        for row in cursor.fetchall():
            tasks.append({
                'task_id': row[0],
                'event_count': row[1],
                'student_count': row[2]
            })
        
        conn.close()
        
        logger.info("[ProcessModelGenerator.get_available_tasks] - Found tasks", count=len(tasks))
        return tasks

    def export_model_pnml(
        self,
        out_path: str,
        noise_threshold: float = 0.2,
        task_id: Optional[str] = None,
    ) -> None:
        """
        Gera o modelo (usando `generate_model`) e exporta para PNML em `out_path`.

        Tenta diferentes rotas de exportação dependendo da versão do PM4Py instalada.

        Args:
            out_path: Caminho do arquivo PNML de saída.
            noise_threshold: Parâmetro do Inductive Miner.
            task_id: task_id opcional para gerar modelo específico.

        Raises:
            ModelGenerationError: se a exportação falhar.
        """
        net, initial_marking, final_marking = self.generate_model(
            noise_threshold=noise_threshold, task_id=task_id
        )

        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Tentativas de exportação compatíveis com diferentes versões do pm4py
        last_error = None
        # 1) modern exporter.apply(net, output_path, initial_marking=..., final_marking=...)
        try:
            from pm4py.objects.petri.exporter import exporter as pnml_exporter
            try:
                pnml_exporter.apply(net, str(out_path), initial_marking=initial_marking, final_marking=final_marking)
                return
            except TypeError as e:
                last_error = e
            # 2) older exporter signature: apply(net, initial_marking, final_marking, output_path)
            try:
                pnml_exporter.apply(net, initial_marking, final_marking, str(out_path))
                return
            except TypeError as e:
                last_error = e
        except Exception as e:
            last_error = e

        # 3) Try pm4py.write_pnml with possible signatures
        try:
            # signature: write_pnml(net, initial_marking, final_marking, file_path)
            try:
                pm4py.write_pnml(net, initial_marking, final_marking, str(out_path))
                return
            except TypeError as e:
                last_error = e
            # signature variant: write_pnml(net, file_path, initial_marking, final_marking)
            try:
                pm4py.write_pnml(net, str(out_path), initial_marking, final_marking)
                return
            except TypeError as e:
                last_error = e
        except Exception as e:
            last_error = e

        # 4) As last resort, try pn_visualizer to generate SVG and skip PNML (not ideal)
        try:
            from pm4py.visualization.petri_net import visualizer as pn_visualizer
            gviz = pn_visualizer.apply(net, initial_marking, final_marking)
            # attempt to save as SVG next to PNML
            svg_path = str(out_path.with_suffix('.svg'))
            pn_visualizer.save(gviz, svg_path)
        except Exception as e:
            # report earlier error
            raise ModelGenerationError(f"Failed to export PNML; last error: {last_error}; svg fallback error: {e}")
        # If reached here, write SVG but PNML export failed
        raise ModelGenerationError(f"PNML export failed for unknown reasons; last error: {last_error}")


if __name__ == '__main__':
    # Pequena CLI para gerar PNMLs por tarefa
    import argparse

    p = argparse.ArgumentParser(description='Generate PNML models from model_events using ProcessModelGenerator')
    p.add_argument('--db', required=True, help='Path to SQLite DB')
    p.add_argument('--out-dir', default='models', help='Directory to write PNML files')
    p.add_argument('--noise', type=float, default=0.2, help='noise_threshold for Inductive Miner')
    p.add_argument('--tasks', help='Comma-separated list of task_ids to generate (default: all)')
    args = p.parse_args()

    generator = ProcessModelGenerator(db_path=args.db)

    if args.tasks:
        task_list = [t.strip() for t in args.tasks.split(',') if t.strip()]
    else:
        task_list = [t['task_id'] for t in generator.get_available_tasks()]

    print(f'Generating PNML models for {len(task_list)} tasks into {args.out_dir} (noise={args.noise})')

    for tid in task_list:
        out_file = Path(args.out_dir) / f"{tid}.pnml"
        try:
            print(f' - Generating model for task {tid} -> {out_file}')
            generator.export_model_pnml(str(out_file), noise_threshold=args.noise, task_id=tid)
        except Exception as e:
            print(f'Failed to generate model for {tid}: {e}')
