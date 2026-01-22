#!/usr/bin/env python3
"""
Coupled Discovery + Replay Runner

Executa descoberta de modelos por tarefa e replay imediato de traces de análise,
sem necessidade de exportar/importar PNML. Repete para múltiplos noise_threshold.

Usage:
  python coupled_discover_and_replay.py --db path/to/src.db --noise 0.1,0.2,0.3 --out-dir outputs

Saída:
    - outputs/replay_metrics_noise_{noise}.csv (métricas por trace)
    - Resumo impresso no stdout
"""

import argparse
import sqlite3
import pandas as pd
import os
import sys
import json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

# PM4Py imports
import pm4py
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.log.obj import EventLog, Trace, Event
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay

# Local imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from process_mining.model_generator import ProcessModelGenerator


def build_event_log_for_task(conn: sqlite3.Connection, task_id: str, table: str = "analysis_events") -> EventLog:
    """
    Constrói um EventLog PM4Py a partir dos eventos de uma tarefa no DB.
    
    Args:
        conn: Conexão SQLite
        task_id: ID da tarefa
        table: Nome da tabela (analysis_events ou model_events)
    
    Returns:
        EventLog PM4Py com traces agrupados por case_id
    """
    query = f"""
    SELECT 
        student_hash,
        task_id,
        activity,
        timestamp,
        event_type,
        metadata
    FROM {table}
    WHERE task_id = ?
    ORDER BY student_hash, timestamp
    """
    
    df = pd.read_sql_query(query, conn, params=(task_id,))
    
    if len(df) == 0:
        return EventLog()
    
    # Agrupa por student_hash para criar traces
    event_log = EventLog()
    
    for student_hash, group in df.groupby('student_hash', sort=False):
        trace = Trace()
        trace.attributes['concept:name'] = f"{student_hash}:{task_id}"
        trace.attributes['student_hash'] = student_hash
        trace.attributes['task_id'] = task_id
        
        for _, row in group.iterrows():
            event = Event()
            event['concept:name'] = row['activity']
            event['time:timestamp'] = pd.to_datetime(row['timestamp'])
            event['org:resource'] = student_hash
            event['task_id'] = task_id
            event['event_type'] = row['event_type']
            
            # Parse metadata JSON se existir
            if pd.notna(row['metadata']) and row['metadata']:
                try:
                    metadata = json.loads(row['metadata'])
                    for key, val in metadata.items():
                        event[f"tko:{key}"] = val
                except:
                    pass
            
            trace.append(event)
        
        if len(trace) > 0:
            event_log.append(trace)
    
    return event_log


def replay_trace_with_model(
    trace: Trace,
    net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking
) -> Dict[str, Any]:
    """
    Faz token-based replay de um trace contra um modelo.
    
    Args:
        trace: Trace PM4Py
        net: Petri Net
        initial_marking: Marcação inicial
        final_marking: Marcação final
    
    Returns:
        Dicionário com métricas por trace
    """
    # Cria EventLog com um único trace
    temp_log = EventLog()
    temp_log.append(trace)
    
    # Token-based replay
    try:
        replayed_traces = token_replay.apply(
            temp_log,
            net,
            initial_marking,
            final_marking
        )
        
        if len(replayed_traces) == 0:
            raise ValueError("No replay result returned")
        
        replay_result = replayed_traces[0]
        
        # Extrai métricas
        trace_fitness = replay_result.get('trace_fitness', 0.0)
        missing = replay_result.get('missing_tokens', 0)
        consumed = replay_result.get('consumed_tokens', 0)
        remaining = replay_result.get('remaining_tokens', 0)
        produced = replay_result.get('produced_tokens', 0)
        
        # Extrai listas de transições
        activated_transitions = replay_result.get('activated_transitions', [])
        transitions_with_problems = replay_result.get('transitions_with_problems', [])
        
        # Serializa listas para JSON (para CSV)
        activated_json = json.dumps([str(t) for t in activated_transitions]) if activated_transitions else "[]"
        problems_json = json.dumps([str(t) for t in transitions_with_problems]) if transitions_with_problems else "[]"
        
        # Calcula trace_length e trace_duration
        trace_length = len(trace)
        
        timestamps = [e['time:timestamp'] for e in trace if 'time:timestamp' in e]
        if len(timestamps) >= 2:
            trace_duration = (max(timestamps) - min(timestamps)).total_seconds()
        else:
            trace_duration = 0.0
        
        return {
            'case_id': trace.attributes.get('concept:name', ''),
            'student_hash': trace.attributes.get('student_hash', ''),
            'task_id': trace.attributes.get('task_id', ''),
            'trace_length': trace_length,
            'trace_duration': trace_duration,
            'trace_fitness': trace_fitness,
            'consumed': consumed,
            'missing': missing,
            'produced': produced,
            'remaining': remaining,
            'activated_transitions': activated_json,
            'transitions_with_problems': problems_json
        }
    
    except Exception as e:
        print(f"Warning: replay failed for trace {trace.attributes.get('concept:name', 'UNKNOWN')}: {e}")
        return None


def process_task(
    task_item,
    noise_threshold: float,
    generator: ProcessModelGenerator,
    db_path: str
) -> List[Dict[str, Any]]:
    """
    Processa uma tarefa: descobre modelo e faz replay de todos os traces de análise.
    
    Args:
        task_id: ID da tarefa
        noise_threshold: Threshold de ruído para Inductive Miner
        generator: Instância do ProcessModelGenerator
        conn: Conexão SQLite
    
    Returns:
        Lista de dicionários com métricas por trace
    """
    # task_item may be a dict (from get_available_tasks) or a plain task_id string
    if isinstance(task_item, dict):
        task_id = task_item.get('task_id') or task_item.get('task')
    else:
        task_id = task_item

    print(f"  Processando tarefa {task_id} (noise={noise_threshold})...")

    try:
        # Descobrir modelo in-memory
        net, initial_marking, final_marking = generator.generate_model(
            noise_threshold=noise_threshold,
            task_id=task_id
        )

        # Carregar traces de análise para essa tarefa (abrir conexão local por worker)
        conn = sqlite3.connect(db_path)
        analysis_log = build_event_log_for_task(conn, task_id, table="analysis_events")
        conn.close()
        
        if len(analysis_log) == 0:
            print(f"    Aviso: Nenhum trace de análise para a tarefa {task_id}")
            return []
        
        # Fazer replay para cada trace
        results = []
        for trace in analysis_log:
            metrics = replay_trace_with_model(trace, net, initial_marking, final_marking)
            if metrics:
                metrics['noise_threshold'] = noise_threshold
                results.append(metrics)
        
        print(f"    ✓ Tarefa {task_id}: {len(results)} traces reexecutados")
        return results
    
    except Exception as e:
        print(f"    ✗ Tarefa {task_id} falhou: {e}")
        return []


def run_coupled_discovery_replay(
    db_path: str,
    noise_thresholds: List[float],
    out_dir: str,
    max_workers: int = 4
) -> None:
    """
    Executa descoberta + replay acoplado para múltiplos noise_threshold.
    
    Args:
        db_path: Caminho do banco de dados SQLite
        noise_thresholds: Lista de thresholds de ruído
        out_dir: Diretório de saída para CSVs
        max_workers: Número de workers para paralelização por tarefa
    """
    os.makedirs(out_dir, exist_ok=True)
    
    generator = ProcessModelGenerator(db_path)
    
    # Obtém lista de tarefas
    available_tasks = generator.get_available_tasks()
    print(f"\nEncontradas {len(available_tasks)} tarefas no conjunto MODEL")
    
    if len(available_tasks) == 0:
        print("Erro: Nenhuma tarefa encontrada na tabela model_events")
        sys.exit(1)
    
    # Para cada noise_threshold
    for noise in noise_thresholds:
        print(f"\n{'='*60}")
        print(f"Processando com noise_threshold = {noise}")
        print(f"{'='*60}")
        
        all_results = []
        
        # Paraleliza por tarefa (Opção A)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_task, task_id, noise, generator, db_path): task_id
                for task_id in available_tasks
            }
            
            for future in as_completed(futures):
                task_id = futures[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    print(f"  Error processing task {task_id}: {e}")
        
        # Salva CSV
        if len(all_results) > 0:
            df = pd.DataFrame(all_results)
            csv_path = os.path.join(out_dir, f"replay_metrics_noise_{noise}.csv")
            df.to_csv(csv_path, index=False)
            print(f"\n✓ Salvo {len(all_results)} métricas de traces em {csv_path}")
            
            # Estatísticas resumidas
            print(f"\nResumo para noise={noise}:")
            print(f"  Total de traces: {len(df)}")
            print(f"  Média de fitness: {df['trace_fitness'].mean():.3f}")
            print(f"  Mediana de fitness: {df['trace_fitness'].median():.3f}")
            print(f"  Mín. fitness: {df['trace_fitness'].min():.3f}")
            print(f"  Máx. fitness: {df['trace_fitness'].max():.3f}")
            print(f"  Traces com fitness < 0.7: {(df['trace_fitness'] < 0.7).sum()}")
        else:
            print(f"\nAviso: Sem resultados para noise={noise}")
    
    # no persistent connection to close; workers open their own connections
    print("\n" + "="*60)
    print("Descoberta acoplada + replay concluídos!")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(description="Coupled Discovery + Replay Runner")
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    parser.add_argument('--noise', default='0.1,0.2,0.3', help='Comma-separated noise thresholds')
    parser.add_argument('--out-dir', default='outputs', help='Output directory for CSV files')
    parser.add_argument('--workers', type=int, default=4, help='Max parallel workers for task processing')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.db):
        print(f"Error: Database not found: {args.db}")
        sys.exit(1)
    
    # Parse noise thresholds
    noise_thresholds = [float(x.strip()) for x in args.noise.split(',')]
    
    print("="*60)
    print("Coupled Discovery + Replay Runner")
    print("="*60)
    print(f"Database: {args.db}")
    print(f"Noise thresholds: {noise_thresholds}")
    print(f"Output directory: {args.out_dir}")
    print(f"Max workers: {args.workers}")
    
    run_coupled_discovery_replay(
        db_path=args.db,
        noise_thresholds=noise_thresholds,
        out_dir=args.out_dir,
        max_workers=args.workers
    )


if __name__ == "__main__":
    main()
