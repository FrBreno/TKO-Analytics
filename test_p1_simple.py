"""
Script de teste simples para Process Discovery (P.1).
"""

import sys
import os
from pathlib import Path

# Fix encoding for Windows console
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Adiciona src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from process_mining.process_discovery import ProcessDiscovery

def main():
    print("="*80)
    print("TKO Process Discovery - Test (P.1)")
    print("="*80)
    
    db_path = Path(__file__).parent / 'src.db'
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return
    
    print(f"Database: {db_path}")
    print()
    
    print("[1/3] Initializing...")
    discovery = ProcessDiscovery(str(db_path))
    print("[OK] Initialized")
    print()
    
    print("[2/3] Discovering process for ALL students...")
    try:
        result = discovery.discover_all_students(compute_conformance=True)
        print("[OK] Discovery completed!")
        print()
        print(f"Students: {result.num_students}")
        print(f"Tasks: {result.num_tasks}")
        print(f"Traces: {result.pm4py_analysis.num_traces}")
        print(f"Events: {result.pm4py_analysis.num_events}")
        print(f"Variants: {result.pm4py_analysis.num_variants}")
        
        if result.pm4py_analysis.fitness:
            print(f"Fitness: {result.pm4py_analysis.fitness:.2%}")
        if result.pm4py_analysis.precision:
            print(f"Precision: {result.pm4py_analysis.precision:.2%}")
        
        print(f"XES file: {result.xes_path}")
        print()
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("[3/3] Testing task-specific discovery (motoca)...")
    try:
        task_result = discovery.discover_task('motoca', compute_conformance=True)
        print("[OK] Task discovery completed!")
        print(f"Students: {task_result.num_students}")
        print(f"Traces: {task_result.pm4py_analysis.num_traces}")
        print(f"Events: {task_result.pm4py_analysis.num_events}")
        print(f"XES file: {task_result.xes_path}")
        print()
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("="*80)
    print("SUCCESS: All tests passed!")
    print("="*80)

if __name__ == '__main__':
    main()
