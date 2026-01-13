"""
Test Script for V.1-V.4 Visualizations

Testa todas as visualizações criadas nas fases V.1 a V.4.
"""

import sys
from pathlib import Path

# Adiciona src ao path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from visualizations import (
    ProcessMapVisualizer,
    TimelineVisualizer,
    SelfAssessmentComparator,
    ActivityHeatmapVisualizer
)

DB_PATH = "src.db"


def test_v1_process_maps():
    """Testa V.1 - Process Map Visualizer."""
    print("\n" + "="*60)
    print("V.1 - PROCESS MAP VISUALIZER")
    print("="*60)
    
    viz = ProcessMapVisualizer(DB_PATH)
    
    # Test 1: Process map geral
    print("\n[Test 1] Generating general process map...")
    fig = viz.generate_process_map()
    fig.write_html("test_outputs/v1_process_map_general.html")
    print("✓ Saved to test_outputs/v1_process_map_general.html")
    
    # Test 2: Process map de uma tarefa específica
    print("\n[Test 2] Generating process map for 'motoca'...")
    fig = viz.generate_process_map(task_id="motoca")
    fig.write_html("test_outputs/v1_process_map_motoca.html")
    print("✓ Saved to test_outputs/v1_process_map_motoca.html")
    
    # Test 3: Activity frequency chart
    print("\n[Test 3] Generating activity frequency chart...")
    fig = viz.generate_activity_frequency_chart()
    fig.write_html("test_outputs/v1_activity_frequency.html")
    print("✓ Saved to test_outputs/v1_activity_frequency.html")
    
    # Test 4: Transition matrix heatmap
    print("\n[Test 4] Generating transition matrix...")
    fig = viz.generate_transition_matrix()
    fig.write_html("test_outputs/v1_transition_matrix.html")
    print("✓ Saved to test_outputs/v1_transition_matrix.html")
    
    print("\n✅ V.1 tests complete!")


def test_v2_timelines():
    """Testa V.2 - Timeline Visualizer."""
    print("\n" + "="*60)
    print("V.2 - TIMELINE VISUALIZER")
    print("="*60)
    
    viz = TimelineVisualizer(DB_PATH)
    
    # Busca um estudante com dados
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT student_hash FROM sessions LIMIT 1")
    student = cursor.fetchone()
    conn.close()
    
    if not student:
        print("⚠️  No student data found")
        return
    
    student_hash = student[0]
    
    # Test 1: Student timeline
    print(f"\n[Test 1] Generating timeline for student {student_hash[:8]}...")
    fig = viz.generate_student_timeline(student_hash)
    fig.write_html("test_outputs/v2_student_timeline.html")
    print("✓ Saved to test_outputs/v2_student_timeline.html")
    
    # Test 2: Task timeline
    print("\n[Test 2] Generating task timeline for 'motoca'...")
    fig = viz.generate_task_timeline("motoca")
    fig.write_html("test_outputs/v2_task_timeline.html")
    print("✓ Saved to test_outputs/v2_task_timeline.html")
    
    # Test 3: Activity over time
    print("\n[Test 3] Generating activity over time chart...")
    fig = viz.generate_activity_over_time()
    fig.write_html("test_outputs/v2_activity_over_time.html")
    print("✓ Saved to test_outputs/v2_activity_over_time.html")
    
    # Test 4: Session duration distribution
    print("\n[Test 4] Generating session duration distribution...")
    fig = viz.generate_session_duration_distribution()
    fig.write_html("test_outputs/v2_session_duration_dist.html")
    print("✓ Saved to test_outputs/v2_session_duration_dist.html")
    
    print("\n✅ V.2 tests complete!")


def test_v3_self_assessment():
    """Testa V.3 - Self-Assessment Comparator."""
    print("\n" + "="*60)
    print("V.3 - SELF-ASSESSMENT COMPARATOR")
    print("="*60)
    
    comparator = SelfAssessmentComparator(DB_PATH)
    
    # Busca um estudante com autoavaliações
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT student_hash 
        FROM events 
        WHERE event_type = 'SelfEvent' 
        LIMIT 1
    """)
    student = cursor.fetchone()
    conn.close()
    
    if not student:
        print("⚠️  No self-assessment data found")
        return
    
    student_hash = student[0]
    
    # Test 1: Time estimates comparison
    print(f"\n[Test 1] Comparing time estimates for student {student_hash[:8]}...")
    fig = comparator.compare_time_estimates(student_hash)
    fig.write_html("test_outputs/v3_time_estimates.html")
    print("✓ Saved to test_outputs/v3_time_estimates.html")
    
    # Test 2: Autonomy claims analysis
    print(f"\n[Test 2] Analyzing autonomy claims for student {student_hash[:8]}...")
    fig = comparator.analyze_autonomy_claims(student_hash)
    fig.write_html("test_outputs/v3_autonomy_claims.html")
    print("✓ Saved to test_outputs/v3_autonomy_claims.html")
    
    # Test 3: Help received comparison
    print("\n[Test 3] Comparing help received for 'motoca'...")
    fig = comparator.compare_help_received("motoca")
    fig.write_html("test_outputs/v3_help_received.html")
    print("✓ Saved to test_outputs/v3_help_received.html")
    
    # Test 4: Generate full report
    print(f"\n[Test 4] Generating self-assessment report...")
    report = comparator.generate_self_assessment_report(student_hash)
    print(f"Report: {report}")
    
    print("\n✅ V.3 tests complete!")


def test_v4_heatmaps():
    """Testa V.4 - Activity Heatmap Visualizer."""
    print("\n" + "="*60)
    print("V.4 - ACTIVITY HEATMAP VISUALIZER")
    print("="*60)
    
    viz = ActivityHeatmapVisualizer(DB_PATH)
    
    # Test 1: Time of day heatmap
    print("\n[Test 1] Generating time-of-day heatmap...")
    fig = viz.generate_time_of_day_heatmap()
    fig.write_html("test_outputs/v4_time_of_day_heatmap.html")
    print("✓ Saved to test_outputs/v4_time_of_day_heatmap.html")
    
    # Test 2: Student x Task heatmap
    print("\n[Test 2] Generating student-task heatmap...")
    fig = viz.generate_student_task_heatmap()
    fig.write_html("test_outputs/v4_student_task_heatmap.html")
    print("✓ Saved to test_outputs/v4_student_task_heatmap.html")
    
    # Test 3: Event type distribution heatmap
    print("\n[Test 3] Generating event type heatmap...")
    fig = viz.generate_event_type_heatmap()
    fig.write_html("test_outputs/v4_event_type_heatmap.html")
    print("✓ Saved to test_outputs/v4_event_type_heatmap.html")
    
    # Test 4: Pattern frequency heatmap
    print("\n[Test 4] Generating pattern frequency heatmap...")
    fig = viz.generate_pattern_frequency_heatmap()
    fig.write_html("test_outputs/v4_pattern_frequency_heatmap.html")
    print("✓ Saved to test_outputs/v4_pattern_frequency_heatmap.html")
    
    # Test 5: Session intensity heatmap
    print("\n[Test 5] Generating session intensity heatmap...")
    fig = viz.generate_session_intensity_heatmap()
    fig.write_html("test_outputs/v4_session_intensity_heatmap.html")
    print("✓ Saved to test_outputs/v4_session_intensity_heatmap.html")
    
    print("\n✅ V.4 tests complete!")


def main():
    """Executa todos os testes de visualização."""
    print("\n" + "="*60)
    print("TESTING VISUALIZATIONS V.1-V.4")
    print("="*60)
    
    # Cria diretório de outputs
    Path("test_outputs").mkdir(exist_ok=True)
    
    try:
        test_v1_process_maps()
        test_v2_timelines()
        test_v3_self_assessment()
        test_v4_heatmaps()
        
        print("\n" + "="*60)
        print("ALL VISUALIZATION TESTS COMPLETE! ✅")
        print("="*60)
        print("\nOutput files saved in test_outputs/ directory:")
        print("  - V.1: process_map_*.html")
        print("  - V.2: *_timeline.html")
        print("  - V.3: *_estimates.html, *_claims.html")
        print("  - V.4: *_heatmap.html")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
