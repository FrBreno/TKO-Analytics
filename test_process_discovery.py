"""
Script de teste para Process Discovery (P.1).

Testa:
- Conversão TKO → XES
- Descoberta de processo com Inductive Miner
- Geração de visualizações (Petri net)
- Análise de variantes
"""

import sys
import os
from pathlib import Path

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Adiciona src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import structlog
from process_mining.process_discovery import ProcessDiscovery

# Configura logger
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()

def main():
    """Executa teste de Process Discovery."""
    
    print("=" * 80)
    print("TKO Process Discovery - Test Script (P.1)")
    print("=" * 80)
    print()
    
    # Caminho do banco de dados
    db_path = Path(__file__).parent / 'src.db'
    
    if not db_path.exists():
        print(f"[ERROR] Database not found: {db_path}")
        sys.exit(1)
    
    print(f"[OK] Database found: {db_path}")
    print()
    
    # Inicializa Process Discovery
    print("[1/4] Initializing Process Discovery...")
    discovery = ProcessDiscovery(str(db_path))
    print("✓ Process Discovery initialized")
    print()
    
    # Descoberta para TODOS os estudantes
    print("[2/4] Discovering process for ALL students...")
    print("   • Converting TKO events → XES format")
    print("   • Applying Inductive Miner")
    print("   • Calculating conformance metrics")
    print()
    
    try:
        result = discovery.discover_all_students(compute_conformance=True)
        
        print("✓ Process Discovery completed!")
        print()
        print(result)
        print()
        
        # Teste descoberta para uma tarefa específica
        print("[3/4] Discovering process for TASK 'motoca'...")
        task_result = discovery.discover_task('motoca', compute_conformance=True)
        
        print("✓ Task discovery completed!")
        print()
        print(f"Task 'motoca' Statistics:")
        print(f"   • Students: {task_result.num_students}")
        print(f"   • Traces: {task_result.pm4py_analysis.num_traces}")
        print(f"   • Events: {task_result.pm4py_analysis.num_events}")
        print(f"   • Process variants: {task_result.pm4py_analysis.num_variants}")
        print(f"   • Fitness: {task_result.pm4py_analysis.fitness:.2%}" if task_result.pm4py_analysis.fitness else "   • Fitness: N/A")
        print(f"   • Precision: {task_result.pm4py_analysis.precision:.2%}" if task_result.pm4py_analysis.precision else "   • Precision: N/A")
        print(f"   • XES file: {task_result.xes_path}")
        print()
        
        # Resumo final
        print("[4/4] Summary of Generated Files:")
        print(f"   • Main XES: {result.xes_path}")
        print(f"   • Task XES: {task_result.xes_path}")
        
        # Lista arquivos de visualização
        xes_dir = Path(result.xes_path).parent
        png_files = list(xes_dir.glob('*.png'))
        if png_files:
            print(f"   • Process models ({len(png_files)} PNG files):")
            for png in png_files:
                print(f"      - {png.name}")
        
        print()
        print("=" * 80)
        print("✓ ALL TESTS PASSED - Process Discovery (P.1) working correctly!")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error during process discovery: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
