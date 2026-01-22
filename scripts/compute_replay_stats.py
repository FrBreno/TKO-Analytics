#!/usr/bin/env python3
"""Resume estatísticas de replay a partir de arquivos `outputs/replay_metrics_noise_*.csv`.

Gera: `outputs/replay_stats_summary.json` e `outputs/agg_by_task_noise_0.2.csv` (quando aplicável).
"""
from pathlib import Path
import argparse
import json
import pandas as pd


def summarize_df(df):
    """Gera um dicionário resumo de estatísticas para a série `trace_fitness`.

    Retorna totais, média, mediana, desvio padrão, mínimo, máximo e contagem/pct abaixo de 0.7.
    """
    if df.empty:
        return {
            'total_traces': 0,
            'mean_fitness': None,
            'median_fitness': None,
            'std_fitness': None,
            'min_fitness': None,
            'max_fitness': None,
            'count_below_0_7': 0,
            'pct_below_0_7': None,
        }
    vals = df['trace_fitness'].astype(float)
    count_below = int((vals < 0.7).sum())
    return {
        'total_traces': int(len(vals)),
        'mean_fitness': round(float(vals.mean()), 4),
        'median_fitness': round(float(vals.median()), 4),
        'std_fitness': round(float(vals.std(ddof=0)), 4) if len(vals) > 1 else 0.0,
        'min_fitness': round(float(vals.min()), 4),
        'max_fitness': round(float(vals.max()), 4),
        'count_below_0_7': count_below,
        'pct_below_0_7': round(100.0 * count_below / len(vals), 4),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--outputs', default=None, help='Path to outputs dir (defaults to TKO-Analytics/outputs)')
    args = p.parse_args()

    base = Path(__file__).resolve().parents[1]
    outputs = Path(args.outputs) if args.outputs else base / 'outputs'
    outputs.mkdir(parents=True, exist_ok=True)

    files = sorted(outputs.glob('replay_metrics_noise_*.csv'))
    summary = {'files': [], 'per_noise': {}, 'notes': []}

    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
        except Exception as e:
            summary['notes'].append(f'failed_read:{f.name}:{e}')
            continue
        noise = f.stem.replace('replay_metrics_noise_', '')
        stats = summarize_df(df) if 'trace_fitness' in df.columns else {'error': 'missing trace_fitness column'}
        summary['files'].append(str(f.name))
        summary['per_noise'][noise] = stats
        # save per-noise aggregated by task for noise 0.2
        if noise == '0.2' and 'task_id' in df.columns and 'trace_fitness' in df.columns:
            agg = df.groupby('task_id').trace_fitness.agg(['count', 'mean', 'median', 'std', 'min', 'max']).reset_index()
            agg = agg.rename(columns={'count': 'n_traces', 'mean': 'mean_fitness', 'median': 'median_fitness', 'std': 'std_fitness'})
            agg['mean_fitness'] = agg['mean_fitness'].astype(float).round(4)
            agg['median_fitness'] = agg['median_fitness'].astype(float).round(4)
            agg['std_fitness'] = agg['std_fitness'].fillna(0.0).astype(float).round(4)
            agg['min'] = agg['min'].astype(float).round(4)
            agg['max'] = agg['max'].astype(float).round(4)
            agg.to_csv(outputs / 'agg_by_task_noise_0.2.csv', index=False)

    # overall across all files (concatenate)
    all_dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            if 'trace_fitness' in df.columns:
                df['__noise'] = f.stem.replace('replay_metrics_noise_', '')
                # ensure required columns exist
                for c in ['case_id','student_hash','task_id','trace_fitness','__noise']:
                    if c not in df.columns and c != '__noise':
                        df[c] = None
                all_dfs.append(df[['case_id','student_hash','task_id','trace_fitness','__noise']].copy())
        except Exception:
            pass
    if all_dfs:
        big = pd.concat(all_dfs, ignore_index=True)
        big['trace_fitness'] = big['trace_fitness'].astype(float)
        overall_stats = summarize_df(big.rename(columns={'trace_fitness':'trace_fitness'}) )
        summary['overall'] = overall_stats
    else:
        summary['overall'] = {'total_traces': 0}

    out = outputs / 'replay_stats_summary.json'
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f'Wrote summary to {out}')


if __name__ == '__main__':
    main()
