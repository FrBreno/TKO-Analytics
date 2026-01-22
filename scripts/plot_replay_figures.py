#!/usr/bin/env python3
"""Gera figuras para resultados de replay: histograma, boxplot por tarefa, heatmap e gráfico de médias por tarefa."""
from pathlib import Path
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def ensure_fig_dir(base):
    d = base / 'figuras'
    d.mkdir(parents=True, exist_ok=True)
    return d


def plot_histogram(df, outpath):
    plt.figure(figsize=(8,4))
    sns.histplot(df['trace_fitness'].dropna(), bins=30)
    plt.xlabel('Fitness')
    plt.title('Distribuição de fitness (todos os traços)')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_boxplot_by_task(df, outpath, max_tasks=20):
    # limita número de tarefas para evitar poluição do gráfico
    counts = df['task_id'].value_counts()
    top_tasks = counts.nlargest(max_tasks).index.tolist()
    sel = df[df['task_id'].isin(top_tasks)].copy()
    plt.figure(figsize=(12,6))
    sns.boxplot(x='task_id', y='trace_fitness', data=sel)
    plt.xticks(rotation=45, ha='right')
    plt.xlabel('Task ID')
    plt.ylabel('Fitness')
    plt.title('Boxplot de fitness por tarefa (top tasks)')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_mean_by_task(df, outpath, top_n=50):
    agg = df.groupby('task_id').trace_fitness.mean().sort_values(ascending=False)
    agg = agg.head(top_n)
    plt.figure(figsize=(10,6))
    sns.barplot(x=agg.values, y=agg.index)
    plt.xlabel('Média de fitness')
    plt.title('Média de fitness por tarefa (top)')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def plot_heatmap(df, outpath, max_students=50, max_tasks=50):
    pivot = df.pivot_table(index='student_hash', columns='task_id', values='trace_fitness', aggfunc='mean')
    pivot = pivot.fillna(0)
    pivot = pivot.loc[pivot.index[:max_students], pivot.columns[:max_tasks]]
    plt.figure(figsize=(12,8))
    sns.heatmap(pivot, cmap='viridis')
    plt.title('Heatmap: student x task (fitness)')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--outputs', default=None)
    args = p.parse_args()
    base = Path(__file__).resolve().parents[1]
    outputs = Path(args.outputs) if args.outputs else base / 'outputs'
    figures = ensure_fig_dir(base)

    # prefere o arquivo com noise 0.2 quando disponível
    f = outputs / 'replay_metrics_noise_0.2.csv'
    if not f.exists():
        files = sorted(outputs.glob('replay_metrics_noise_*.csv'))
        if not files:
            print('No replay files found')
            return
        f = files[0]

    df = pd.read_csv(f, low_memory=False)
    if 'trace_fitness' not in df.columns:
        print('trace_fitness not in columns')
        return

    plot_histogram(df, figures / 'fitness_histograma.png')
    plot_boxplot_by_task(df, figures / 'fitness_boxplot_por_tarefa.png')
    plot_mean_by_task(df, figures / 'fitness_media_por_tarefa.png')
    plot_heatmap(df, figures / 'fitness_heatmap.png')
    print(f'Wrote figures to {figures}')


if __name__ == '__main__':
    main()
