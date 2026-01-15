"""
Demonstração de Process Mining com PM4Py.

Este script demonstra:
1. Exportação XES de eventos TKO
2. Importação do XES com PM4Py
3. Descoberta de modelo de processo (Inductive Miner)
4. Análise de variantes
5. Métricas de conformidade
6. Visualização do modelo

Uso:
    python demo_process_mining.py
"""

import tempfile
from pathlib import Path

from src.exporters import export_to_xes
from src.process_mining import ProcessAnalyzer
from src.etl.init_db import init_database
from src.etl.loader import SQLiteLoader
from src.models.events import ExecEvent, MoveEvent, SelfEvent
from datetime import datetime, timedelta


def create_sample_events():
    """Cria eventos de exemplo para demonstração."""
    print("\nCriando eventos de exemplo...")
    
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    # Simula 2 estudantes trabalhando em 2 tarefas
    events = []
    
    # Estudante 1 - Task 1 (sucesso rápido)
    events.extend([
        MoveEvent(timestamp=base_time, task_id="calc", action="PICK"),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=2),
            task_id="calc",
            mode="FULL",
            rate=80,
            size=50
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=5),
            task_id="calc",
            mode="FULL",
            rate=100,
            size=55
        ),
        SelfEvent(
            timestamp=base_time + timedelta(minutes=7),
            task_id="calc",
            rate=100,
            autonomy=9
        ),
    ])
    
    # Estudante 1 - Task 2 (várias tentativas)
    events.extend([
        MoveEvent(
            timestamp=base_time + timedelta(minutes=10),
            task_id="animal",
            action="PICK"
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=12),
            task_id="animal",
            mode="FULL",
            rate=30,
            size=60,
            error="COMP"
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=15),
            task_id="animal",
            mode="FULL",
            rate=60,
            size=70
        ),
        ExecEvent(
            timestamp=base_time + timedelta(minutes=18),
            task_id="animal",
            mode="FULL",
            rate=100,
            size=75
        ),
        SelfEvent(
            timestamp=base_time + timedelta(minutes=20),
            task_id="animal",
            rate=100,
            autonomy=8,
            help_guide="yes"
        ),
    ])
    
    # Estudante 2 - Task 1 (padrão similar ao Estudante 1)
    base_time_2 = base_time + timedelta(hours=1)
    events.extend([
        MoveEvent(timestamp=base_time_2, task_id="calc", action="PICK"),
        ExecEvent(
            timestamp=base_time_2 + timedelta(minutes=3),
            task_id="calc",
            mode="FULL",
            rate=70,
            size=50
        ),
        ExecEvent(
            timestamp=base_time_2 + timedelta(minutes=6),
            task_id="calc",
            mode="FULL",
            rate=100,
            size=55
        ),
        SelfEvent(
            timestamp=base_time_2 + timedelta(minutes=8),
            task_id="calc",
            rate=100,
            autonomy=10
        ),
    ])
    
    print(f"   ✓ {len(events)} eventos criados")
    print(f"   ✓ 2 estudantes, 2 tarefas")
    
    return events


def setup_database_and_export(tmpdir: Path):
    """Configura banco e exporta XES."""
    db_path = tmpdir / "pm_demo.db"
    xes_path = tmpdir / "pm_demo.xes"
    
    print("\nConfigurando banco de dados...")
    init_database(str(db_path))
    
    # Cria eventos
    events = create_sample_events()
    
    # Carrega no banco
    print("\nCarregando eventos no banco...")
    loader = SQLiteLoader(str(db_path))
    
    # Estudante 1
    student1_events = events[:9]
    loader.load_events(student1_events, "student_1", "case_1")
    
    # Estudante 2
    student2_events = events[9:]
    loader.load_events(student2_events, "student_2", "case_2")
    
    print(f"   ✓ {len(events)} eventos carregados")
    
    # Exporta XES
    print("\nExportando para XES...")
    stats = export_to_xes(str(db_path), str(xes_path))
    print(f"   ✓ {stats['events']} eventos exportados")
    print(f"   ✓ {stats['traces']} traces criados")
    print(f"   ✓ Arquivo: {xes_path}")
    
    return db_path, xes_path


def demonstrate_process_mining(xes_path: Path):
    """Demonstra análise de Process Mining."""
    print("\n\n" + "=" * 60)
    print("PROCESS MINING COM PM4PY")
    print("=" * 60)
    
    # Inicializa analisador
    print("\nInicializando Process Analyzer...")
    analyzer = ProcessAnalyzer()
    
    # Análise completa
    print("\nExecutando analise completa...")
    result = analyzer.analyze(
        str(xes_path),
        discover_model=True,
        compute_conformance=True,
        top_variants=5
    )
    
    # Exibe resultados
    print("\n" + "=" * 60)
    print(result)
    print("=" * 60)
    
    # Salva visualização do modelo (opcional)
    try:
        model_path = xes_path.parent / "process_model.png"
        print(f"\nSalvando visualizacao do modelo...")
        analyzer.save_model_visualization(str(model_path), format='png')
        print(f"   ✓ Modelo salvo em: {model_path}")
    except Exception as e:
        print(f"   AVISO: Visualizacao nao disponivel (requer Graphviz): {e}")
    
    return result


def main():
    """Executa demonstração completa."""
    print("\n" + "=" * 60)
    print("TKO ANALYTICS - Process Mining Demo")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Setup
        db_path, xes_path = setup_database_and_export(tmpdir)
        
        # Process Mining
        result = demonstrate_process_mining(xes_path)
        
        # Resumo
        print("\n\n" + "=" * 60)
        print("RESUMO DA DEMONSTRACAO")
        print("=" * 60)
        print(f"\n✓ Process Mining executado com sucesso!")
        print(f"✓ {result.num_events} eventos analisados")
        print(f"✓ {result.num_traces} traces processados")
        print(f"✓ {result.num_variants} variantes de processo identificadas")
        
        if result.fitness and result.precision:
            print(f"✓ Fitness: {result.fitness:.2%}")
            print(f"✓ Precision: {result.precision:.2%}")
        
        print(f"\nArquivos temporarios:")
        print(f"   • Database: {db_path}")
        print(f"   • XES: {xes_path}")
        
        print(f"\nInsights:")
        print(f"   • {result.num_activities} atividades distintas no processo")
        print(f"   • Duração média de trace: {result.avg_trace_duration_seconds:.1f}s")
        print(f"   • Variante mais comum representa {(result.top_variants[0][1]/result.num_traces)*100:.1f}% dos casos")
        
        print("\n" + "=" * 60)
        print("Demo concluída! Arquivos temporários serão removidos.\n")


if __name__ == "__main__":
    main()
