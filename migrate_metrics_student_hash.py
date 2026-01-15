"""
Script de Migração: Corrigir student_hash na tabela metrics

Problema:
- student_hash em metrics estava sendo gerado como SHA256(student_hash_do_banco)[:8] (double-hash truncado)
- student_hash nas outras tabelas é SHA256(student_id) (64 caracteres, single-hash)
- Isso causava incompatibilidade entre as tabelas

Solução:
- Atualizar student_hash nas métricas para corresponder ao student_hash dos eventos
- Buscar valor correto da tabela analysis_events baseado no case_id

Uso:
    python migrate_metrics_student_hash.py
"""

import sqlite3
import sys
from pathlib import Path

# Adiciona src ao path para imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import structlog

logger = structlog.get_logger()

DB_PATH = Path(__file__).parent / 'src.db'


def migrate_metrics_student_hash():
    """Atualiza student_hash nas métricas para corresponder aos eventos."""
    
    if not DB_PATH.exists():
        logger.error("Database not found", path=str(DB_PATH))
        print(f"❌ Banco de dados não encontrado: {DB_PATH}")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # 1. Contar métricas antes da migração
        cursor.execute("SELECT COUNT(*) FROM metrics")
        total_metrics = cursor.fetchone()[0]
        
        if total_metrics == 0:
            print("✅ Nenhuma métrica encontrada para migrar.")
            return
        
        print(f"📊 Total de métricas no banco: {total_metrics}")
        
        # 2. Contar quantas estão inconsistentes
        cursor.execute("""
            SELECT COUNT(DISTINCT m.case_id)
            FROM metrics m
            JOIN analysis_events e ON m.case_id = e.case_id
            WHERE m.student_hash != e.student_hash
        """)
        inconsistent_cases = cursor.fetchone()[0]
        
        if inconsistent_cases == 0:
            print("✅ Todas as métricas já estão com student_hash correto.")
            return
        
        print(f"⚠️  Cases com student_hash inconsistente: {inconsistent_cases}")
        print(f"\n🔄 Iniciando migração...")
        
        # 3. Buscar todos os cases únicos em metrics
        cursor.execute("SELECT DISTINCT case_id FROM metrics")
        case_ids = [row[0] for row in cursor.fetchall()]
        
        migrated_metrics = 0
        not_found = 0
        already_correct = 0
        
        for case_id in case_ids:
            # Buscar student_hash correto dos eventos
            cursor.execute("""
                SELECT DISTINCT student_hash 
                FROM analysis_events 
                WHERE case_id = ?
                LIMIT 1
            """, (case_id,))
            
            result = cursor.fetchone()
            
            if result:
                correct_student_hash = result[0]
                
                # Verificar quantas métricas deste case_id têm hash incorreto
                cursor.execute("""
                    SELECT COUNT(*) FROM metrics
                    WHERE case_id = ? AND student_hash != ?
                """, (case_id, correct_student_hash))
                
                incorrect_count = cursor.fetchone()[0]
                
                if incorrect_count == 0:
                    cursor.execute("""
                        SELECT COUNT(*) FROM metrics WHERE case_id = ?
                    """, (case_id,))
                    already_correct += cursor.fetchone()[0]
                    continue
                
                # Atualizar todas as métricas deste case_id
                cursor.execute("""
                    UPDATE metrics 
                    SET student_hash = ? 
                    WHERE case_id = ?
                """, (correct_student_hash, case_id))
                
                migrated_metrics += incorrect_count
                
                if migrated_metrics % 10 == 0:
                    print(f"  ✓ {migrated_metrics} métricas migradas...")
            else:
                # Tentar na tabela model_events
                cursor.execute("""
                    SELECT DISTINCT student_hash 
                    FROM model_events 
                    WHERE case_id = ?
                    LIMIT 1
                """, (case_id,))
                
                result = cursor.fetchone()
                
                if result:
                    correct_student_hash = result[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM metrics
                        WHERE case_id = ? AND student_hash != ?
                    """, (case_id, correct_student_hash))
                    
                    incorrect_count = cursor.fetchone()[0]
                    
                    if incorrect_count == 0:
                        cursor.execute("""
                            SELECT COUNT(*) FROM metrics WHERE case_id = ?
                        """, (case_id,))
                        already_correct += cursor.fetchone()[0]
                        continue
                    
                    cursor.execute("""
                        UPDATE metrics 
                        SET student_hash = ? 
                        WHERE case_id = ?
                    """, (correct_student_hash, case_id))
                    
                    migrated_metrics += incorrect_count
                    
                    if migrated_metrics % 10 == 0:
                        print(f"  ✓ {migrated_metrics} métricas migradas...")
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM metrics WHERE case_id = ?
                    """, (case_id,))
                    orphan_count = cursor.fetchone()[0]
                    
                    logger.warning("Metrics without matching events",
                                 case_id=case_id,
                                 count=orphan_count)
                    not_found += orphan_count
        
        # 4. Commit das mudanças
        conn.commit()
        
        # 5. Relatório final
        print("\n" + "="*60)
        print("✅ MIGRAÇÃO CONCLUÍDA")
        print("="*60)
        print(f"Total de métricas:        {total_metrics}")
        print(f"Já estavam corretas:      {already_correct}")
        print(f"Migradas com sucesso:     {migrated_metrics}")
        print(f"Não encontradas:          {not_found}")
        print("="*60)
        
        if migrated_metrics > 0:
            print("\n🎯 Agora o student_hash nas métricas corresponde aos eventos!")
            print("   JOINs entre 'metrics' e 'events' funcionarão corretamente.")
        
        if not_found > 0:
            print(f"\n⚠️  {not_found} métricas não têm eventos correspondentes.")
            print("   Considere remover essas métricas órfãs.")
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Migration failed", error=str(e))
        print(f"\n❌ Erro durante migração: {e}")
        raise
    
    finally:
        conn.close()


def verify_migration():
    """Verifica se a migração foi bem-sucedida."""
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # Verificar se há inconsistências
        cursor.execute("""
            SELECT COUNT(DISTINCT m.case_id)
            FROM metrics m
            JOIN analysis_events e ON m.case_id = e.case_id
            WHERE m.student_hash != e.student_hash
        """)
        
        inconsistent = cursor.fetchone()[0]
        
        if inconsistent == 0:
            print("\n✅ VERIFICAÇÃO: Todas as métricas têm student_hash consistente!")
        else:
            print(f"\n⚠️  VERIFICAÇÃO: {inconsistent} cases ainda inconsistentes")
            
        # Mostrar exemplo de valores corretos
        cursor.execute("""
            SELECT 
                m.case_id,
                m.student_hash as metrics_hash,
                e.student_hash as events_hash,
                m.task_id,
                m.metric_name
            FROM metrics m
            JOIN analysis_events e ON m.case_id = e.case_id
            WHERE m.student_hash = e.student_hash
            LIMIT 3
        """)
        
        examples = cursor.fetchall()
        if examples:
            print("\n📋 Exemplos de valores após migração:")
            for case_id, m_hash, e_hash, task_id, metric_name in examples:
                print(f"  ✅ {case_id} | {task_id} | {metric_name}")
                print(f"     Metrics:  {m_hash}")
                print(f"     Events:   {e_hash}")
                print()
            
    finally:
        conn.close()


if __name__ == "__main__":
    print("="*60)
    print("🔧 MIGRAÇÃO: Corrigir student_hash em metrics")
    print("="*60)
    print()
    
    migrate_metrics_student_hash()
    verify_migration()
    
    print("\n✨ Migração finalizada!\n")
