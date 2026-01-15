"""
Demonstração End-to-End do Pipeline ETL + Métricas TKO Analytics.

Este script demonstra o fluxo completo:
1. Parse de CSV → Modelos Pydantic
2. Validação de eventos
3. Detecção de sessões
4. Carregamento no SQLite
5. Cálculo de métricas pedagógicas
6. Consultas e estatísticas

Uso:
    python demo_pipeline.py
"""

import json
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
    """Cria arquivo CSV de exemplo com eventos TKO."""
    print("\n📄 Criando arquivo CSV de exemplo...")
    
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    # Simula uma sessão de trabalho completa em uma tarefa
    lines = [
        # Header
        "timestamp,task,mode,rate,size,error,autonomy,help_human,help_iagen,help_guide,help_other,study",
        
        # Aluno começa navegando pelas tarefas
        f"{base_time.isoformat()},calculadora,DOWN,,,,,,,,,,",
        f"{(base_time + timedelta(seconds=5)).isoformat()},calculadora,PICK,,,,,,,,,,",
        
        # Primeira tentativa - falha de compilação
        f"{(base_time + timedelta(minutes=2)).isoformat()},calculadora,FULL,30,50,COMP,,,,,,,",
        
        # Segunda tentativa - passa alguns testes
        f"{(base_time + timedelta(minutes=5)).isoformat()},calculadora,FULL,60,75,NONE,,,,,,,",
        
        # Terceira tentativa - melhora mas não 100%
        f"{(base_time + timedelta(minutes=8)).isoformat()},calculadora,FULL,85,90,NONE,,,,,,,",
        
        # Execução livre para debug
        f"{(base_time + timedelta(minutes=10)).isoformat()},calculadora,FREE,,100,NONE,,,,,,,",
        
        # Tentativa final - sucesso!
        f"{(base_time + timedelta(minutes=15)).isoformat()},calculadora,FULL,100,100,NONE,,,,,,,",
        
        # Auto-avaliação
        f"{(base_time + timedelta(minutes=16)).isoformat()},calculadora,SELF,100,,,8,professor,chatgpt,,outros_materiais,120",
        
        # Navega para próxima tarefa
        f"{(base_time + timedelta(minutes=17)).isoformat()},animal,PICK,,,,,,,,,,",
        
        # Animal - resolve rápido (tarefa fácil)
        f"{(base_time + timedelta(minutes=20)).isoformat()},animal,FULL,100,80,NONE,,,,,,,",
        f"{(base_time + timedelta(minutes=21)).isoformat()},animal,SELF,100,,,10,,,,,60",
        
        # Tenta tarefa difícil
        f"{(base_time + timedelta(minutes=22)).isoformat()},carro,DOWN,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=23)).isoformat()},carro,PICK,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=30)).isoformat()},carro,FULL,50,120,NONE,,,,,,,",
        f"{(base_time + timedelta(minutes=40)).isoformat()},carro,FULL,75,150,NONE,,,,,,,",
        
        # Volta para tarefa anterior (BACK)
        f"{(base_time + timedelta(minutes=42)).isoformat()},animal,BACK,,,,,,,,,,",
        
        # Edita código anterior
        f"{(base_time + timedelta(minutes=43)).isoformat()},animal,EDIT,,,,,,,,,,",
        f"{(base_time + timedelta(minutes=45)).isoformat()},animal,FULL,100,85,NONE,,,,,,,",
    ]
    
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"   ✓ CSV criado: {csv_path}")
    print(f"   ✓ {len(lines) - 1} eventos (3 tarefas: calculadora, animal, carro)")


def demonstrate_parsing(csv_path: Path):
    """Demonstra parsing de CSV."""
    print("\n🔍 FASE 1: Parsing CSV → Modelos Pydantic")
    print("=" * 60)
    
    parser = LogParser(strict=False)  # Non-strict para coletar erros sem travar
    events = parser.parse_file(csv_path)
    
    print(f"\n✓ Parseados {len(events)} eventos")
    
    if parser.errors:
        print(f"⚠ {len(parser.errors)} erros encontrados:")
        for error in parser.errors[:3]:  # Mostra apenas primeiros 3
            print(f"   - {error}")
    
    # Estatísticas por tipo
    exec_count = sum(1 for e in events if isinstance(e, ExecEvent))
    move_count = sum(1 for e in events if isinstance(e, MoveEvent))
    self_count = sum(1 for e in events if isinstance(e, SelfEvent))
    
    print(f"\n📊 Distribuição de eventos:")
    print(f"   • ExecEvent: {exec_count}")
    print(f"   • MoveEvent: {move_count}")
    print(f"   • SelfEvent: {self_count}")
    
    # Mostra exemplo de cada tipo
    print(f"\n📋 Exemplos de eventos:")
    for event_type, event in [(ExecEvent, None), (MoveEvent, None), (SelfEvent, None)]:
        sample = next((e for e in events if isinstance(e, event_type)), None)
        if sample:
            print(f"\n   {type(sample).__name__}:")
            print(f"      timestamp: {sample.timestamp}")
            print(f"      task_id: {sample.task_id}")
            if isinstance(sample, ExecEvent):
                print(f"      mode: {sample.mode}, rate: {sample.rate}%, size: {sample.size}")
            elif isinstance(sample, MoveEvent):
                print(f"      action: {sample.action}")
            elif isinstance(sample, SelfEvent):
                print(f"      autonomy: {sample.autonomy}/10, has_help: {sample.has_any_help()}")
    
    return events


def demonstrate_validation(events):
    """Demonstra validação de eventos."""
    print("\n\n✅ FASE 2: Validação de Integridade")
    print("=" * 60)
    
    validator = EventValidator(
        check_timestamps=True,
        check_duplicates=True,
        check_value_ranges=True,
        allow_backwards_time=False
    )
    
    report = validator.validate(events)
    
    print(f"\n{report.summary()}")
    print(f"\n📊 Estatísticas de validação:")
    print(f"   • Total de eventos: {report.total_events}")
    print(f"   • Eventos válidos: {report.valid_events}")
    print(f"   • Taxa de erro: {report.error_rate:.1%}")
    
    if report.errors:
        print(f"\n❌ Erros encontrados ({len(report.errors)}):")
        for error in report.errors[:5]:  # Mostra primeiros 5
            print(f"   [{error.error_type}] Event #{error.event_index}: {error.message}")
    
    if report.warnings:
        print(f"\n⚠️  Warnings ({len(report.warnings)}):")
        for warning in report.warnings[:5]:
            print(f"   [{warning.error_type}] Event #{warning.event_index}: {warning.message}")
    
    return report


def demonstrate_loading(events, db_path: Path):
    """Demonstra carregamento no SQLite."""
    print("\n\n💾 FASE 3: Carregamento no SQLite")
    print("=" * 60)
    
    # Inicializa banco
    print("\n📦 Inicializando banco de dados...")
    init_database(str(db_path))
    print(f"   ✓ Banco criado: {db_path}")
    
    # Carrega eventos
    print("\n📥 Carregando eventos...")
    loader = SQLiteLoader(str(db_path), batch_size=100)
    
    count = loader.load_events(
        events,
        student_id="aluno_demo_001",
        case_id="demo_case_2024",
        session_id="session_001"
    )
    
    print(f"   ✓ {count} eventos carregados")
    print(f"   ✓ {loader.events_skipped} eventos skipados (duplicatas)")
    
    return loader


def demonstrate_sessions(events, db_path: Path):
    """Demonstra detecção de sessões."""
    print("\n\n⏱️  FASE 4: Detecção de Sessões")
    print("=" * 60)
    
    detector = SessionDetector(timeout_minutes=30)
    
    sessions = detector.detect_sessions(
        events,
        case_id="demo_case_2024",
        student_id="aluno_demo_001"
    )
    
    print(f"\n✓ Detectadas {len(sessions)} sessões de trabalho")
    
    for i, session in enumerate(sessions, 1):
        duration_min = session.duration_seconds // 60
        print(f"\n   Sessão {i} ({session.task_id}):")
        print(f"      Duração: {duration_min} minutos")
        print(f"      Eventos: {session.event_count}")
        print(f"      Execuções: {session.exec_count}")
        print(f"      Navegações: {session.move_count}")
        print(f"      Auto-avaliações: {session.self_count}")
    
    # Salva no banco
    detector.save_sessions(sessions, str(db_path))
    print(f"\n✓ Sessões salvas no banco de dados")
    
    return sessions


def demonstrate_metrics(events, sessions, db_path: Path):
    """Demonstra cálculo de métricas pedagógicas."""
    print("\n\n📊 FASE 5: Cálculo de Métricas Pedagógicas")
    print("=" * 60)
    
    engine = MetricsEngine(session_timeout_minutes=30)
    
    metrics = engine.compute_all_metrics(
        events=events,
        sessions=sessions,
        case_id="demo_case_2024",
        student_id="aluno_demo_001",
        task_id="mixed_tasks"
    )
    
    print(f"\n✓ Calculadas {len(metrics)} métricas")
    
    # Agrupa métricas por categoria
    temporal = [m for m in metrics if "time" in m.metric_name or "session" in m.metric_name]
    performance = [m for m in metrics if "success" in m.metric_name or "attempt" in m.metric_name or "trajectory" in m.metric_name]
    behavioral = [m for m in metrics if "ratio" in m.metric_name or "detected" in m.metric_name]
    self_assessment = [m for m in metrics if "autonomy" in m.metric_name or "help" in m.metric_name]
    
    print(f"\n📈 Métricas Temporais ({len(temporal)}):")
    for m in temporal:
        value = f"{m.metric_value:.0f}" if m.metric_value >= 1 else f"{m.metric_value:.2f}"
        print(f"   • {m.metric_name}: {value}")
    
    print(f"\n🎯 Métricas de Desempenho ({len(performance)}):")
    for m in performance:
        if m.metadata and "pattern" in m.metadata:
            print(f"   • {m.metric_name}: {m.metadata['pattern']}")
        else:
            value = f"{m.metric_value:.0f}" if m.metric_value >= 1 else f"{m.metric_value:.2f}"
            print(f"   • {m.metric_name}: {value}")
    
    print(f"\n🔍 Métricas Comportamentais ({len(behavioral)}):")
    for m in behavioral:
        if isinstance(m.metric_value, float) and m.metric_value in [0.0, 1.0]:
            status = "✓ Sim" if m.metric_value == 1.0 else "✗ Não"
            print(f"   • {m.metric_name}: {status}")
        else:
            print(f"   • {m.metric_name}: {m.metric_value:.2f}")
    
    print(f"\n🤔 Métricas de Auto-Avaliação ({len(self_assessment)}):")
    for m in self_assessment:
        print(f"   • {m.metric_name}: {m.metric_value:.2f}")
    
    # Salva no banco
    engine.save_metrics(metrics, str(db_path))
    print(f"\n✓ Métricas salvas no banco de dados")
    
    return metrics


def demonstrate_queries(loader, case_id: str):
    """Demonstra consultas ao banco."""
    print("\n\n🔎 FASE 6: Consultas e Análises")
    print("=" * 60)
    
    # Contagem total
    total = loader.get_event_count()
    print(f"\n📊 Total de eventos no banco: {total}")
    
    # Contagem por case
    case_count = loader.get_event_count(case_id=case_id)
    print(f"   • Eventos do case '{case_id}': {case_count}")
    
    # Recupera eventos por task
    print("\n📋 Eventos da tarefa 'calculadora':")
    calc_events = loader.get_events(case_id=case_id, task_id="calculadora", limit=10)
    
    for i, event in enumerate(calc_events[:5], 1):
        metadata = json.loads(event["metadata"])
        print(f"   {i}. [{event['event_type']}] {event['activity']}")
        print(f"      timestamp: {event['timestamp']}")
        
        if event['event_type'] == 'ExecEvent':
            print(f"      mode: {metadata['mode']}, rate: {metadata.get('rate', 'N/A')}%")
        elif event['event_type'] == 'SelfEvent':
            print(f"      autonomy: {metadata.get('autonomy', 'N/A')}/10")
    
    # Estatísticas por atividade
    print("\n📈 Análise temporal:")
    all_events = loader.get_events(case_id=case_id, limit=1000)
    
    if len(all_events) >= 2:
        first_ts = datetime.fromisoformat(all_events[0]["timestamp"])
        last_ts = datetime.fromisoformat(all_events[-1]["timestamp"])
        duration = last_ts - first_ts
        
        print(f"   • Início: {first_ts.strftime('%H:%M:%S')}")
        print(f"   • Fim: {last_ts.strftime('%H:%M:%S')}")
        print(f"   • Duração total: {duration}")
    
    # Agrupa por tipo de atividade
    activity_counts = {}
    for event in all_events:
        activity = event["activity"]
        activity_counts[activity] = activity_counts.get(activity, 0) + 1
    
    print(f"\n📊 Distribuição por atividade:")
    for activity, count in sorted(activity_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   • {activity}: {count} eventos")
    
    # Tasks únicas
    unique_tasks = set(event["task_id"] for event in all_events)
    print(f"\n🎯 Tarefas trabalhadas: {len(unique_tasks)}")
    for task in sorted(unique_tasks):
        task_events = [e for e in all_events if e["task_id"] == task]
        print(f"   • {task}: {len(task_events)} eventos")


def demonstrate_xes_export(db_path: Path, tmpdir: Path) -> Path:
    """Demonstra exportação para formato XES (IEEE 1849.2016)."""
    print("\n\n" + "=" * 60)
    print("FASE 6: EXPORTAÇÃO XES (Process Mining)")
    print("=" * 60)
    
    xes_path = tmpdir / "tko_events.xes"
    
    print(f"\n📊 Exportando eventos para formato XES...")
    print(f"   Standard: IEEE XES 1849.2016")
    print(f"   Compatível com: PM4Py, ProM, Disco")
    
    # Exporta todos os eventos
    stats = export_to_xes(str(db_path), str(xes_path))
    
    print(f"\n✅ Exportação concluída!")
    print(f"   • {stats['events']} eventos exportados")
    print(f"   • {stats['traces']} traces criados")
    print(f"   • {stats['cases']} cases únicos")
    print(f"   • Arquivo: {xes_path.name}")
    print(f"   • Tamanho: {xes_path.stat().st_size / 1024:.2f} KB")
    
    # Mostra início do arquivo XES
    print(f"\n📄 Preview do arquivo XES:")
    with open(xes_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()[:15]
        for line in lines:
            print(f"   {line.rstrip()}")
        if len(lines) == 15:
            print("   ...")
    
    print(f"\n💡 Para carregar no PM4Py:")
    print(f"   import pm4py")
    print(f"   log = pm4py.read_xes('{xes_path.name}')")
    print(f"   net, im, fm = pm4py.discover_petri_net_inductive(log)")
    
    return xes_path


def main():
    """Executa demonstração completa."""
    print("\n" + "=" * 60)
    print("🚀 TKO ANALYTICS - Pipeline ETL + Métricas Demo")
    print("=" * 60)
    
    # Usa diretório output persistente
    script_dir = Path(__file__).parent
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)
    
    csv_path = output_dir / "demo_data.csv"
    db_path = output_dir / "demo_tko.db"
    
    # Pipeline completo
    create_sample_csv(csv_path)
    events = demonstrate_parsing(csv_path)
    report = demonstrate_validation(events)
    loader = demonstrate_loading(events, db_path)
    sessions = demonstrate_sessions(events, db_path)
    metrics = demonstrate_metrics(events, sessions, db_path)
    xes_path = demonstrate_xes_export(db_path, output_dir)
    demonstrate_queries(loader, case_id="demo_case_2024")
        
    # Resumo final
    print("\n\n" + "=" * 60)
    print("✨ RESUMO DA DEMONSTRAÇÃO")
    print("=" * 60)
    print(f"\n✓ Pipeline executado com sucesso!")
    print(f"✓ {len(events)} eventos processados")
    print(f"✓ {report.valid_events} eventos válidos")
    print(f"✓ {loader.events_loaded} eventos carregados no banco")
    print(f"✓ {len(sessions)} sessões detectadas")
    print(f"✓ {len(metrics)} métricas calculadas")
    print(f"✓ {xes_path.stat().st_size / 1024:.2f} KB exportados em XES")
    print(f"\n📁 Arquivos temporários:")
    print(f"   • CSV: {csv_path}")
    print(f"   • SQLite: {db_path}")
    print(f"   • XES: {xes_path}")
    print(f"\n💡 Próximos passos:")
    print(f"   1. ✅ Exportação XES implementada (IEEE 1849.2016)")
    print(f"   2. Integrar análise PM4Py (process discovery)")
    print(f"   3. Criar dashboard de visualização Flask")
    print(f"   4. Análise estatística com cohort real")
    
    print("\n" + "=" * 60)
    print("Demo concluída! Arquivos temporários serão removidos.\n")


if __name__ == "__main__":
    main()
