"""
Script de teste completo para Fases P.1, P.2 e P.3.

Testa:
- P.1: Process Discovery (XES conversion, Inductive Miner)
- P.2: Conformance Checking (modelo ideal TKO, fitness, precision)
- P.3: Behavioral Pattern Detection (cramming, trial-and-error, procrastinação, code thrashing)
"""

import sys
import os
from pathlib import Path

# Fix encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from process_mining.process_discovery import ProcessDiscovery
from process_mining.conformance_checker import ConformanceChecker
from process_mining.pattern_detector import BehavioralPatternDetector

def main():
    print("="*80)
    print("TKO Process Mining - Complete Test (P.1 + P.2 + P.3)")
    print("="*80)
    print()
    
    db_path = Path(__file__).parent / 'src.db'
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return
    
    print(f"Database: {db_path}")
    print()
    
    # ========================================================================
    # FASE P.1: PROCESS DISCOVERY
    # ========================================================================
    print("[P.1] PROCESS DISCOVERY")
    print("-" * 80)
    
    print("[P.1.1] Initializing Process Discovery...")
    discovery = ProcessDiscovery(str(db_path))
    print("[OK] Initialized")
    print()
    
    print("[P.1.2] Discovering process for task 'motoca'...")
    try:
        task_result = discovery.discover_task('motoca', compute_conformance=True)
        print("[OK] Discovery completed!")
        print(f"   Students: {task_result.num_students}")
        print(f"   Traces: {task_result.pm4py_analysis.num_traces}")
        print(f"   Events: {task_result.pm4py_analysis.num_events}")
        print(f"   Variants: {task_result.pm4py_analysis.num_variants}")
        
        if task_result.pm4py_analysis.fitness:
            print(f"   Fitness: {task_result.pm4py_analysis.fitness:.2%}")
        if task_result.pm4py_analysis.precision:
            print(f"   Precision: {task_result.pm4py_analysis.precision:.2%}")
        
        print(f"   XES: {task_result.xes_path}")
        print()
        
    except Exception as e:
        print(f"[ERROR] P.1 failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ========================================================================
    # FASE P.2: CONFORMANCE CHECKING
    # ========================================================================
    print("[P.2] CONFORMANCE CHECKING")
    print("-" * 80)
    
    print("[P.2.1] Initializing Conformance Checker...")
    checker = ConformanceChecker()
    print("[OK] Initialized")
    print()
    
    print("[P.2.2] Defining ideal TKO process model...")
    ideal_net, ideal_im, ideal_fm = checker.define_ideal_tko_model()
    print(f"[OK] Ideal model created:")
    print(f"   Places: {len(ideal_net.places)}")
    print(f"   Transitions: {len(ideal_net.transitions)}")
    print(f"   Arcs: {len(ideal_net.arcs)}")
    print()
    
    print("[P.2.3] Checking conformance for task 'motoca'...")
    try:
        conf_result = checker.check_conformance_from_xes(
            xes_path=task_result.xes_path,
            task_id='motoca'
        )
        
        print("[OK] Conformance analysis completed!")
        print(f"   Conformance Level: {conf_result.conformance_level}")
        print(f"   Fitness: {conf_result.fitness:.2%}")
        print(f"   Precision: {conf_result.precision:.2%}")
        print(f"   Deviations: {conf_result.num_deviations}")
        if conf_result.deviation_types:
            print(f"   Deviation Types: {', '.join(conf_result.deviation_types)}")
        print()
        
    except Exception as e:
        print(f"[ERROR] P.2 failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ========================================================================
    # FASE P.3: BEHAVIORAL PATTERN DETECTION
    # ========================================================================
    print("[P.3] BEHAVIORAL PATTERN DETECTION")
    print("-" * 80)
    
    print("[P.3.1] Initializing Pattern Detector...")
    detector = BehavioralPatternDetector(str(db_path))
    print("[OK] Initialized")
    print()
    
    print("[P.3.2] Detecting patterns for first 5 students...")
    try:
        # Busca primeiros 5 estudantes
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT student_hash
            FROM events
            WHERE task_id = 'motoca'
            LIMIT 5
        """)
        students = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"[INFO] Testing with {len(students)} students")
        print()
        
        all_patterns = []
        
        for i, student_hash in enumerate(students, 1):
            print(f"   [{i}/{len(students)}] Student {student_hash[:8]}...")
            patterns = detector.detect_all_patterns(student_hash, 'motoca')
            all_patterns.extend(patterns)
            
            if patterns:
                print(f"      [FOUND] {len(patterns)} pattern(s):")
                for pattern in patterns:
                    print(f"         - {pattern.pattern_type} (confidence: {pattern.confidence:.1%})")
            else:
                print(f"      [NO PATTERNS]")
        
        print()
        print(f"[OK] Pattern detection completed!")
        print(f"   Total patterns detected: {len(all_patterns)}")
        
        # Estatísticas por tipo
        if all_patterns:
            pattern_counts = {}
            for p in all_patterns:
                pattern_counts[p.pattern_type] = pattern_counts.get(p.pattern_type, 0) + 1
            
            print(f"   Pattern distribution:")
            for ptype, count in pattern_counts.items():
                print(f"      - {ptype}: {count}")
        
        print()
        
        # Salva padrões no banco
        if all_patterns:
            print("[P.3.3] Saving patterns to database...")
            saved = detector.save_patterns_to_db(all_patterns)
            print(f"[OK] Saved {saved} patterns")
            print()
        
    except Exception as e:
        print(f"[ERROR] P.3 failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ========================================================================
    # RESUMO FINAL
    # ========================================================================
    print("="*80)
    print("SUCCESS: All phases completed!")
    print("="*80)
    print()
    print("Summary:")
    print(f"   [P.1] Process Discovery: OK")
    print(f"      - XES file: {Path(task_result.xes_path).name}")
    print(f"      - Traces: {task_result.pm4py_analysis.num_traces}")
    print(f"      - Variants: {task_result.pm4py_analysis.num_variants}")
    print()
    print(f"   [P.2] Conformance Checking: OK")
    print(f"      - Level: {conf_result.conformance_level}")
    print(f"      - Fitness: {conf_result.fitness:.1%}")
    print(f"      - Precision: {conf_result.precision:.1%}")
    print()
    print(f"   [P.3] Pattern Detection: OK")
    print(f"      - Patterns found: {len(all_patterns)}")
    print(f"      - Students analyzed: {len(students)}")
    print()
    print("="*80)

if __name__ == '__main__':
    main()
