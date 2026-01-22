#!/usr/bin/env python3
"""Agrega métricas de replay por tarefa e por estudante e calcula estatísticas resumo.

Uso:
    python aggregate_stats.py --replay-csv outputs/replay_metrics.csv --events-csv outputs/events_prepared.csv --out-dir outputs

Este script produz:
 - outputs/agg_by_task.csv
 - outputs/agg_by_student.csv
 - outputs/fitness_histogram_by_task.csv (resumos)
"""
import argparse
import pandas as pd
import os


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--replay-csv', required=True)
    p.add_argument('--events-csv', required=True)
    p.add_argument('--out-dir', required=True)
    args = p.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    replay = pd.read_csv(args.replay_csv, keep_default_na=False)
    events = pd.read_csv(args.events_csv, parse_dates=['time:timestamp'], keep_default_na=False)

    # tenta agregar comprimento e duração das traces
    # Calcula comprimento da trace e duração por case a partir dos eventos
    tinfo = events.groupby('case_id').agg(
        trace_length=('activity', 'count'),
        start_time=('time:timestamp', 'min'),
        end_time=('time:timestamp', 'max')
    ).reset_index()
    try:
        tinfo['trace_duration_s'] = (pd.to_datetime(tinfo['end_time']) - pd.to_datetime(tinfo['start_time'])).dt.total_seconds()
    except Exception:
        tinfo['trace_duration_s'] = None

    replay = replay.merge(tinfo, how='left', on='case_id')

    # converte colunas numéricas
    for c in ['trace_fitness', 'consumed_tokens', 'missing_tokens', 'produced_tokens', 'remaining_tokens']:
        if c in replay.columns:
            replay[c] = pd.to_numeric(replay[c], errors='coerce')

    # agregação por tarefa
    agg_task = replay.groupby('task_id').agg(
        n_traces=('case_id', 'nunique'),
        fitness_mean=('trace_fitness', 'mean'),
        fitness_median=('trace_fitness', 'median'),
        fitness_std=('trace_fitness', 'std'),
        pct_low_fitness=('trace_fitness', lambda x: (x < 0.7).sum() / x.count() if x.count() > 0 else 0)
    ).reset_index()

    # agregação por estudante (estudante inferido a partir do split de `case_id`)
    replay['student_hash'] = replay['case_id'].astype(str).apply(lambda x: x.split(':', 1)[0])
    agg_student = replay.groupby('student_hash').agg(
        n_traces=('case_id', 'nunique'),
        fitness_mean=('trace_fitness', 'mean'),
        fitness_median=('trace_fitness', 'median')
    ).reset_index()

    agg_task.to_csv(os.path.join(args.out_dir, 'agg_by_task.csv'), index=False)
    agg_student.to_csv(os.path.join(args.out_dir, 'agg_by_student.csv'), index=False)
    replay.to_csv(os.path.join(args.out_dir, 'replay_with_traceinfo.csv'), index=False)

    print('Escreveu arquivos de agregação em', args.out_dir)


if __name__ == '__main__':
    main()
