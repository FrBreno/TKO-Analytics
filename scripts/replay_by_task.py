#!/usr/bin/env python3
"""Executa token-based replay por trace usando modelos PNML em models/{task_id}.pnml.

Uso:
    python replay_by_task.py --db path/to/db.sqlite --events-csv outputs/events_prepared.csv \
            --models-dir models --out-csv outputs/replay_metrics.csv

Observações:
 - Requer `pm4py` instalado. O script tentará importar o importador PNML e o algoritmo de token replay
     do `pm4py` e exibirá mensagem explicativa caso a biblioteca não esteja disponível.
 - Os modelos devem estar como arquivos PNML nomeados `task_id.pnml` dentro de `--models-dir`.
"""
import argparse
import sqlite3
import pandas as pd
import os
import json
import sys
from datetime import datetime

try:
    from pm4py.objects.log.obj import EventLog, Trace, Event
    from pm4py.objects.petri.importer import importer as pnml_importer
    from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
except Exception as e:
    print('pm4py imports failed. Please ensure pm4py is installed. Error:', e)
    print('You can install with: pip install pm4py')
    sys.exit(2)


def build_event_log(df):
    """Constrói um `EventLog` do pandas DataFrame de eventos agrupando por `case_id`.

    Preserva ordenação por timestamp quando disponível e anexa atributos `tko:*` quando presentes.
    """
    log = EventLog()
    for case_id, g in df.groupby('case_id'):
        trace = Trace()
        # ordena por timestamp quando disponível
        if 'time:timestamp' in g.columns and not g['time:timestamp'].isna().all():
            g = g.sort_values('time:timestamp')
        for _, row in g.iterrows():
            ev = Event({})
            ev['concept:name'] = row['activity']
            if 'time:timestamp' in row and pd.notnull(row['time:timestamp']):
                ev['time:timestamp'] = row['time:timestamp']
            # anexa atributos tko se existirem
            for k in ['tko:rate', 'tko:size', 'tko:mode', 'tko:metadata']:
                if k in row.index:
                    ev[k] = row[k]
            trace.append(ev)
        trace.attributes['concept:name'] = case_id
        log.append(trace)
    return log


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=False)
    p.add_argument('--events-csv', required=True)
    p.add_argument('--models-dir', required=True)
    p.add_argument('--out-csv', required=True)
    args = p.parse_args()

    if not os.path.exists(args.events_csv):
        print('CSV de eventos não encontrado:', args.events_csv)
        sys.exit(2)

    df = pd.read_csv(args.events_csv, parse_dates=['time:timestamp'], keep_default_na=False)

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)

    results = []

    for task_id, df_task in df.groupby('task_id'):
        model_path = os.path.join(args.models_dir, f"{task_id}.pnml")
        if not os.path.exists(model_path):
            print('PNML do modelo não encontrado para a tarefa', task_id, 'esperado em', model_path, '; pulando tarefa')
            continue
            print('Processando tarefa', task_id, 'com', df_task['case_id'].nunique(), 'traces')
        try:
            net, initial_marking, final_marking = pnml_importer.apply(model_path)
        except Exception as e:
            print('Failed to import PNML for', task_id, 'error:', e)
            continue

        # build event log for this task
        log = build_event_log(df_task)
        if len(log) == 0:
            continue

        try:
            replay_results = token_replay.apply(log, net, initial_marking, final_marking)
        except Exception as e:
            print('Token replay falhou para a tarefa', task_id, 'erro:', e)
            continue

        # replay_results is a list of dicts - one per trace
        for res in replay_results:
            case_id = res.get('case', None) or res.get('trace', None) or res.get('case_id', None)
            # fallback: pm4py sometimes returns the trace as object; extract by position if missing
            if case_id is None:
                # try to iterate traces
                pass
            row = {
                'case_id': res.get('case_id') or res.get('case') or None,
                'task_id': task_id,
                'trace_fitness': res.get('trace_fitness'),
                'consumed_tokens': res.get('consumed_tokens'),
                'missing_tokens': res.get('missing_tokens'),
                'produced_tokens': res.get('produced_tokens'),
                'remaining_tokens': res.get('remaining_tokens'),
                'activated_transitions': json.dumps(res.get('activated_transitions', []), ensure_ascii=False),
                'transitions_with_problems': json.dumps(res.get('transitions_with_problems', []), ensure_ascii=False),
                'raw_result': json.dumps({k: v for k, v in res.items() if k not in ['activated_transitions', 'transitions_with_problems']}, default=str, ensure_ascii=False)
            }
            results.append(row)

    if len(results) > 0:
        out_df = pd.DataFrame(results)
        out_df.to_csv(args.out_csv, index=False)
        print('Escreveu métricas de replay em', args.out_csv)
    else:
        print('Nenhum resultado de replay foi produzido.')


if __name__ == '__main__':
    main()
