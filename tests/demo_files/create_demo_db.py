"""
Script para criar um banco de dados de demonstração persistente.

Este script cria um banco de dados SQLite com dados de exemplo
que podem ser usados para testar o dashboard.
"""
from datetime import datetime, timedelta
from pathlib import Path

from src import (
    LogParser,
    EventValidator,
    SQLiteLoader,
    SessionDetector,
    MetricsEngine,
    ExecEvent,
    MoveEvent,
    SelfEvent
)
from src.etl.init_db import init_database
from src.exporters import export_to_xes


def create_sample_csv(csv_path: Path) -> None:
    """Cria arquivo CSV de exemplo com mais eventos para demonstração."""
    print("\n📄 Criando arquivo CSV de exemplo...")
    
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    lines = [
        "timestamp,task,mode,rate,size,error,autonomy,help_human,help_iagen,help_guide,help_other,study",
        
        # Aluno 1 - Tarefa calculadora (sessão completa)
        f"{base_time.isoformat()},calculadora,DOWN,,,,,,,,,,",
        f"{(base_time + timedelta(seconds=5)).isoformat()},calculadora,PICK,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=2)).isoformat()},calculadora,FULL,30,50,,8,0,0,1,0,0",
        f"{(base_time + timedelta(minutes=5)).isoformat()},calculadora,FULL,60,50,,8,0,0,0,0,0",
        f"{(base_time + timedelta(minutes=8)).isoformat()},calculadora,FULL,85,50,,9,0,0,0,0,0",
        f"{(base_time + timedelta(minutes=12)).isoformat()},calculadora,FULL,100,50,,10,0,0,0,0,0",
        f"{(base_time + timedelta(minutes=16)).isoformat()},calculadora,SELF,100,,,9,0,0,0,0,1",
        
        # Aluno 1 - Tarefa animal (sessão rápida)
        f"{(base_time + timedelta(minutes=20)).isoformat()},animal,DOWN,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=20, seconds=5)).isoformat()},animal,PICK,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=22)).isoformat()},animal,FULL,100,30,,10,0,0,0,0,0",
        f"{(base_time + timedelta(minutes=24)).isoformat()},animal,SELF,100,,,10,0,0,0,0,1",
        
        # Aluno 1 - Tarefa carro (dificuldade)
        f"{(base_time + timedelta(minutes=26)).isoformat()},carro,DOWN,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=26, seconds=5)).isoformat()},carro,PICK,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=28)).isoformat()},carro,FULL,20,70,COMP,6,1,0,0,0,0",
        f"{(base_time + timedelta(minutes=35)).isoformat()},carro,FULL,50,70,EXEC,7,0,0,1,0,0",
        f"{(base_time + timedelta(minutes=44)).isoformat()},carro,FULL,100,70,,9,0,0,0,0,0",
        
        # Aluno 1 - Volta para animal (revisão)
        f"{(base_time + timedelta(minutes=50)).isoformat()},animal,DOWN,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=50, seconds=5)).isoformat()},animal,PICK,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=53)).isoformat()},animal,FULL,100,30,,10,0,0,0,0,0",
    ]
    
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"   ✓ CSV criado: {csv_path}")
    print(f"   ✓ {len(lines) - 1} eventos criados")


def main():
    """Executa pipeline completo e salva banco persistente."""
    print("=" * 60)
    print("Criando Banco de Dados de Demonstracao para Dashboard")
    print("=" * 60)
    
    # Define caminhos persistentes
    script_dir = Path(__file__).parent
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)
    
    csv_path = output_dir / "demo_data.csv"
    db_path = output_dir / "tko_analytics.db"
    xes_path = output_dir / "tko_events.xes"
    
    # Remove banco anterior se existir
    if db_path.exists():
        db_path.unlink()
        print(f"\n🗑️  Banco anterior removido")
    
    # Cria CSV de exemplo
    create_sample_csv(csv_path)
    
    # 1. Parse
    print("\n🔍 FASE 1: Parsing CSV → Modelos Pydantic")
    print("=" * 60)
    parser = LogParser()
    events = parser.parse_file(csv_path)
    print(f"✓ Parseados {len(events)} eventos")
    
    # 2. Validação
    print("\n✅ FASE 2: Validação de Integridade")
    print("=" * 60)
    validator = EventValidator()
    validation_report = validator.validate(events)
    print(f"✓ {len(events)} eventos válidos")
    
    # 3. Inicializar banco
    print("\n💾 FASE 3: Inicializando Banco de Dados")
    print("=" * 60)
    init_database(db_path)
    print(f"✓ Banco criado: {db_path}")
    
    # 4. Carregar eventos
    print("\n📥 FASE 4: Carregando Eventos")
    print("=" * 60)
    loader = SQLiteLoader(db_path=db_path)
    case_id = "demo_case_2024"
    student_hash = "demo_student_a9873ad6"
    
    loader.load_events(
        case_id=case_id,
        student_id=student_hash,
        events=events
    )
    print(f"✓ {len(events)} eventos carregados")
    
    # 5. Detectar sessões
    print("\n⏱️  FASE 5: Detecção de Sessões")
    print("=" * 60)
    detector = SessionDetector()
    sessions = detector.detect_sessions(
        events=events,
        case_id=case_id,
        student_id=student_hash
    )
    detector.save_sessions(sessions=sessions, db_path=db_path)
    print(f"✓ {len(sessions)} sessões detectadas e salvas")
    
    # 6. Calcular métricas
    print("\n📊 FASE 6: Cálculo de Métricas")
    print("=" * 60)
    engine = MetricsEngine()
    metrics = engine.compute_all_metrics(
        events=events,
        sessions=sessions,
        case_id=case_id,
        student_id=student_hash,
        task_id="mixed_tasks"
    )
    engine.save_metrics(metrics=metrics, db_path=db_path)
    print(f"✓ {len(metrics)} métricas calculadas e salvas")
    
    # 7. Exportar XES
    print("\n📤 FASE 7: Exportação XES")
    print("=" * 60)
    export_to_xes(
        db_path=db_path,
        output_path=xes_path
    )
    print(f"OK XES exportado: {xes_path}")
    
    # Resumo
    print("\n" + "=" * 60)
    print("BANCO DE DADOS CRIADO COM SUCESSO!")
    print("=" * 60)
    print(f"\nArquivos criados:")
    print(f"   - CSV: {csv_path}")
    print(f"   - SQLite: {db_path}")
    print(f"   - XES: {xes_path}")
    print(f"\n💡 Para iniciar o dashboard:")
    print(f"   python serve.py {db_path.relative_to(script_dir.parent.parent)}")
    print(f"\n   Ou use o caminho absoluto:")
    print(f"   python serve.py {db_path.absolute()}")
    print(f"\n🌐 Acesse: http://localhost:5000")
    print()


if __name__ == "__main__":
    main()
