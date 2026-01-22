#!/usr/bin/env python3
"""
Compara fitness entre diferentes thresholds de ruído

Lê múltiplos arquivos `replay_metrics_noise_{noise}.csv` e produz uma tabela comparativa
mostrando média/mediana de fitness por tarefa para diferentes valores de threshold de ruído.

Uso:
    python compare_fitness_by_noise.py --input-dir outputs --output outputs/fitness_comparison.csv

Saída:
    CSV com colunas: task_id, noise_0.1_mean, noise_0.1_median, noise_0.2_mean, ...
"""

import argparse
import os
import sys
import glob
import pandas as pd
from pathlib import Path


def load_replay_metrics(input_dir: str) -> dict:
    """
    Carrega todos os CSVs replay_metrics_noise_*.csv do diretório.
    
    Returns:
        Dicionário {noise_threshold: DataFrame}
    """
    pattern = os.path.join(input_dir, "replay_metrics_noise_*.csv")
    files = glob.glob(pattern)
    
    if len(files) == 0:
        print(f"Error: No replay_metrics_noise_*.csv files found in {input_dir}")
        sys.exit(1)
    
    data_by_noise = {}
    
    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            noise_str = filename.replace("replay_metrics_noise_", "").replace(".csv", "")
            noise_val = float(noise_str)
        except ValueError:
            print(f"Warning: Could not parse noise value from {filename}, skipping")
            continue
        
        df = pd.read_csv(filepath)
        data_by_noise[noise_val] = df
        print(f"Loaded {len(df)} traces for noise={noise_val}")
    
    return data_by_noise


def compute_comparison_table(data_by_noise: dict) -> pd.DataFrame:
    """
    Gera tabela comparativa de fitness por task_id e noise threshold.
    
    Returns:
        DataFrame com colunas: task_id, noise_{noise}_mean, noise_{noise}_median, ...
    """
    # Coletar todas as task_ids únicas
    all_tasks = set()
    for df in data_by_noise.values():
        all_tasks.update(df['task_id'].unique())
    
    all_tasks = sorted(all_tasks)
    
    # Construir tabela comparativa
    comparison = []
    
    for task_id in all_tasks:
        row = {'task_id': task_id}
        
        for noise in sorted(data_by_noise.keys()):
            df = data_by_noise[noise]
            task_df = df[df['task_id'] == task_id]
            
            if len(task_df) > 0:
                mean_fitness = task_df['trace_fitness'].mean()
                median_fitness = task_df['trace_fitness'].median()
                n_traces = len(task_df)
                low_fitness_count = (task_df['trace_fitness'] < 0.7).sum()
            else:
                mean_fitness = None
                median_fitness = None
                n_traces = 0
                low_fitness_count = 0
            
            row[f'noise_{noise}_mean'] = mean_fitness
            row[f'noise_{noise}_median'] = median_fitness
            row[f'noise_{noise}_n_traces'] = n_traces
            row[f'noise_{noise}_low_fitness'] = low_fitness_count
        
        comparison.append(row)
    
    return pd.DataFrame(comparison)


def compute_overall_stats(data_by_noise: dict) -> pd.DataFrame:
    """
    Gera tabela de estatísticas gerais por noise threshold.
    
    Returns:
        DataFrame com colunas: noise, total_traces, mean_fitness, median_fitness, ...
    """
    stats = []
    
    for noise in sorted(data_by_noise.keys()):
        df = data_by_noise[noise]
        
        stats.append({
            'noise_threshold': noise,
            'total_traces': len(df),
            'mean_fitness': df['trace_fitness'].mean(),
            'median_fitness': df['trace_fitness'].median(),
            'std_fitness': df['trace_fitness'].std(),
            'min_fitness': df['trace_fitness'].min(),
            'max_fitness': df['trace_fitness'].max(),
            'traces_low_fitness_lt_0.7': (df['trace_fitness'] < 0.7).sum(),
            'pct_low_fitness': (df['trace_fitness'] < 0.7).sum() / len(df) * 100
        })
    
    return pd.DataFrame(stats)


def main():
    parser = argparse.ArgumentParser(description="Compare fitness across noise thresholds")
    parser.add_argument('--input-dir', default='outputs', help='Directory containing replay_metrics_noise_*.csv files')
    parser.add_argument('--output', default='outputs/fitness_comparison.csv', help='Output CSV file')
    parser.add_argument('--stats', default='outputs/fitness_stats_by_noise.csv', help='Overall stats CSV file')
    
    args = parser.parse_args()
    
    print("="*60)
    print("Fitness Comparison Across Noise Thresholds")
    print("="*60)
    print(f"Input directory: {args.input_dir}")
    print(f"Output file: {args.output}")
    print()
    
    data_by_noise = load_replay_metrics(args.input_dir)
    
    if len(data_by_noise) == 0:
        print("Error: No valid replay metrics files found")
        sys.exit(1)
    
    print("\nGenerating comparison table by task...")
    comparison_df = compute_comparison_table(data_by_noise)
    
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    comparison_df.to_csv(args.output, index=False)
    print(f"✓ Saved comparison table to {args.output}")
    
    print("\nGenerating overall statistics...")
    stats_df = compute_overall_stats(data_by_noise)
    stats_df.to_csv(args.stats, index=False)
    print(f"✓ Saved overall stats to {args.stats}")
    
    print("\n" + "="*60)
    print("Overall Statistics by Noise Threshold")
    print("="*60)
    print(stats_df.to_string(index=False))
    
    print("\n" + "="*60)
    print("Sample of Comparison Table (first 10 tasks)")
    print("="*60)
    print(comparison_df.head(10).to_string(index=False))
    
    print("\nDone!")


if __name__ == "__main__":
    main()
