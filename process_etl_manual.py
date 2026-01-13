"""Script para processar ETL manualmente via linha de comando."""
import sys
import time
import sqlite3
from datetime import datetime
from src.etl.session_detector import SessionDetector
from src.metrics.engine import MetricsEngine
from src.models.events import ExecEvent, MoveEvent, SelfEvent
import structlog

logger = structlog.get_logger()

def process_etl(db_path='src.db'):
    """Processa ETL completo."""
    print("\n" + "="*60)
    print("PROCESSAMENTO ETL")
    print("="*60)
    
    start_time = time.time()
    conn = sqlite3.connect(db_path)
    
    # Verificar eventos
    count = conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    print(f"\n✓ Eventos no banco: {count:,}")
    
    if count == 0:
        print("❌ Nenhum evento para processar")
        conn.close()
        return
    
    # Limpar sessões e métricas
    print("\n→ Limpando sessões e métricas anteriores...")
    conn.execute('DELETE FROM sessions')
    conn.execute('DELETE FROM metrics')
    conn.commit()
    
    # Buscar eventos
    print("→ Carregando eventos...")
    rows = conn.execute("""
        SELECT 
            student_hash, case_id, task_id, event_type,
            timestamp, metadata
        FROM events
        ORDER BY student_hash, timestamp
    """).fetchall()
    
    # Agrupar por estudante
    print("→ Agrupando eventos por estudante...")
    students_events = {}
    for row in rows:
        student_hash = row[0]
        if student_hash not in students_events:
            students_events[student_hash] = []
        
        timestamp = datetime.fromisoformat(row[4])
        
        # Parse metadata JSON se existir
        import json
        metadata = {}
        if row[5]:
            try:
                metadata = json.loads(row[5])
            except:
                pass
        
        event_type = row[3]
        
        if event_type in ('exec', 'ExecEvent'):
            event = ExecEvent(
                timestamp=timestamp,
                k=row[2],  # task_id usando alias
                mode=metadata.get('mode', 'FULL'),
                rate=metadata.get('rate', 0),
                size=metadata.get('size', 1),  # Tamanho do código
                error=metadata.get('error', 'NONE')
            )
        elif event_type in ('move', 'MoveEvent'):
            event = MoveEvent(
                timestamp=timestamp,
                k=row[2],  # task_id usando alias
                action=metadata.get('action', 'DOWN')
            )
        elif event_type in ('self', 'SelfEvent'):
            event = SelfEvent(
                timestamp=timestamp,
                k=row[2],  # task_id usando alias
                rate=metadata.get('rate', 0)
            )
        else:
            continue
        
        students_events[student_hash].append(event)
    
    print(f"✓ {len(students_events)} estudantes encontrados")
    
    # Processar cada estudante
    session_detector = SessionDetector(timeout_minutes=30)
    metrics_engine = MetricsEngine(session_timeout_minutes=30)
    
    total_sessions = 0
    total_metrics = 0
    
    print("\n→ Processando estudantes:")
    for idx, (student_hash, events) in enumerate(students_events.items(), 1):
        print(f"  [{idx:2d}/{len(students_events)}] {student_hash[:8]}... ({len(events)} eventos)", end=' ')
        
        # Detectar sessões por case_id
        cases = {}
        case_id = rows[0][1] if rows else f"case_{student_hash[:8]}"  # Usar case_id do primeiro evento
        
        for event in events:
            if case_id not in cases:
                cases[case_id] = []
            cases[case_id].append(event)
        
        all_sessions = []
        for case_id, case_events in cases.items():
            sessions = session_detector.detect_sessions(
                events=case_events,
                case_id=case_id,
                student_id=student_hash
            )
            all_sessions.extend(sessions)
        
        # Inserir sessões
        for session in all_sessions:
            conn.execute(
                """
                INSERT INTO sessions (
                    id, case_id, student_hash, task_id,
                    start_timestamp, end_timestamp, duration_seconds,
                    event_count, exec_count, move_count, self_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                session.to_db_row()
            )
        total_sessions += len(all_sessions)
        
        # Calcular métricas por tarefa
        tasks = {}
        for event in events:
            if event.task_id not in tasks:
                tasks[event.task_id] = []
            tasks[event.task_id].append(event)
        
        metrics_count = 0
        for task_id, task_events in tasks.items():
            task_sessions = [s for s in all_sessions if s.task_id == task_id]
            # case_id foi obtido anteriormente
            
            metrics = metrics_engine.compute_all_metrics(
                events=task_events,
                sessions=task_sessions,
                case_id=case_id,
                student_id=student_hash,
                task_id=task_id
            )
            
            for metric in metrics:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO metrics (
                        id, case_id, student_hash, task_id,
                        metric_name, metric_value, metadata, computed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    metric.to_db_row()
                )
            metrics_count += len(metrics)
        
        total_metrics += metrics_count
        print(f"→ {len(all_sessions)} sessões, {metrics_count} métricas")
    
    conn.commit()
    conn.close()
    
    elapsed = time.time() - start_time
    
    print("\n" + "="*60)
    print("PROCESSAMENTO CONCLUÍDO")
    print("="*60)
    print(f"✓ Estudantes: {len(students_events)}")
    print(f"✓ Sessões criadas: {total_sessions:,}")
    print(f"✓ Métricas calculadas: {total_metrics:,}")
    print(f"✓ Tempo: {elapsed:.1f}s")
    print("="*60 + "\n")

if __name__ == "__main__":
    try:
        process_etl()
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
