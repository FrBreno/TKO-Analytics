#!/usr/bin/env python3
"""Conta tarefas e traces da tabela de eventos SQLite e prepara o `case_id`.

Uso:
    python counts_and_prep.py --db path/to/db.sqlite --out-csv outputs/events_prepared.csv

O script grava CSV(s) com um conjunto de colunas normalizadas e imprime `n_tasks` e `n_traces`.
"""
import argparse
import sqlite3
import pandas as pd
import os
import sys


def detect_columns(df):
    """Detecta e mapeia nomes de colunas heurísticas para colunas internas.

    Retorna um dicionário com chaves: 'student_hash', 'task_id', 'activity', 'timestamp' quando encontradas.
    """
    cols = {c.lower(): c for c in df.columns}
    mapping = {}
    # heuristics for student hash
    for key in ['student_hash', 'org:resource', 'org_resource', 'student']:
        if key in cols:
            mapping['student_hash'] = cols[key]
            break
    # task id
    for key in ['task_id', 'tko:task_id', 'task']:
        if key in cols:
            mapping['task_id'] = cols[key]
            break
    # activity/concept
    for key in ['concept:name', 'concept_name', 'activity', 'concept']:
        if key in cols:
            mapping['activity'] = cols[key]
            break
    # timestamp
    for key in ['time:timestamp', 'timestamp', 'time', 'time_timestamp']:
        if key in cols:
            mapping['timestamp'] = cols[key]
            break
    return mapping


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', required=True)
    p.add_argument('--out-csv-model', default='outputs/events_model_prepared.csv')
    p.add_argument('--out-csv-analysis', default='outputs/events_analysis_prepared.csv')
    args = p.parse_args()

    if not os.path.exists(args.db):
        print('DB not found:', args.db)
        sys.exit(2)

    conn = sqlite3.connect(args.db)

    # Read model_events
    try:
        df_model = pd.read_sql_query('SELECT * FROM model_events', conn)
    except Exception as e:
        print('Failed to read table `model_events` from DB:', e)
        df_model = pd.DataFrame()

    # Read analysis_events
    try:
        df_analysis = pd.read_sql_query('SELECT * FROM analysis_events', conn)
    except Exception as e:
        print('Failed to read table `analysis_events` from DB:', e)
        df_analysis = pd.DataFrame()

    if df_model.empty and df_analysis.empty:
        print('Neither model_events nor analysis_events could be read or they are empty.')
        sys.exit(3)

    # normaliza e prepara eventos do conjunto 'model_events'
    if not df_model.empty:
        mapping_model = detect_columns(df_model)
        if 'student_hash' not in mapping_model or 'task_id' not in mapping_model or 'activity' not in mapping_model:
            print('Could not detect required columns in `model_events`. Available columns:')
            print(list(df_model.columns))
            print('Please ensure `model_events` contains student_hash, task_id, concept:name and timestamp columns.')
            sys.exit(4)

        df_model['student_hash'] = df_model[mapping_model['student_hash']].astype(str)
        df_model['task_id'] = df_model[mapping_model['task_id']].astype(str)
        df_model['activity'] = df_model[mapping_model['activity']].astype(str)
        if 'timestamp' in mapping_model:
            df_model['time:timestamp'] = pd.to_datetime(df_model[mapping_model['timestamp']])
        else:
            df_model['time:timestamp'] = pd.NaT
        df_model['case_id'] = df_model['student_hash'] + ':' + df_model['task_id']

        os.makedirs(os.path.dirname(args.out_csv_model), exist_ok=True)
        df_model.to_csv(args.out_csv_model, index=False)

        n_tasks = int(df_model['task_id'].nunique())
        n_traces_model = int(df_model['case_id'].nunique())
    else:
        n_tasks = 0
        n_traces_model = 0

    # normaliza e prepara eventos do conjunto 'analysis_events'
    if not df_analysis.empty:
        mapping_analysis = detect_columns(df_analysis)
        if 'student_hash' not in mapping_analysis or 'task_id' not in mapping_analysis or 'activity' not in mapping_analysis:
            print('Could not detect required columns in `analysis_events`. Available columns:')
            print(list(df_analysis.columns))
            print('Please ensure `analysis_events` contains student_hash, task_id, concept:name and timestamp columns.')
            sys.exit(5)

        df_analysis['student_hash'] = df_analysis[mapping_analysis['student_hash']].astype(str)
        df_analysis['task_id'] = df_analysis[mapping_analysis['task_id']].astype(str)
        df_analysis['activity'] = df_analysis[mapping_analysis['activity']].astype(str)
        if 'timestamp' in mapping_analysis:
            df_analysis['time:timestamp'] = pd.to_datetime(df_analysis[mapping_analysis['timestamp']])
        else:
            df_analysis['time:timestamp'] = pd.NaT
        df_analysis['case_id'] = df_analysis['student_hash'] + ':' + df_analysis['task_id']

        os.makedirs(os.path.dirname(args.out_csv_analysis), exist_ok=True)
        df_analysis.to_csv(args.out_csv_analysis, index=False)

        n_traces_analysis = int(df_analysis['case_id'].nunique())
    else:
        n_traces_analysis = 0

    # Imprime resultados esperados para a tese
    print('n_tasks (do model_events):', n_tasks)
    print('n_traces_model (case_id distintos em model_events):', n_traces_model)
    print('n_traces_analysis (case_id distintos em analysis_events):', n_traces_analysis)


if __name__ == '__main__':
    main()
