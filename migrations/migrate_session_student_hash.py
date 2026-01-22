"""
Script de Migração: Corrigir student_hash na tabela sessions

Problema:
- student_hash nas sessões estava sendo gerado como SHA256(SHA256(student_id)) (double-hash)
- student_hash nos eventos é SHA256(student_id) (single-hash)
- Isso causava incompatibilidade entre as tabelas

Solução:
- Atualizar student_hash nas sessões para corresponder ao student_hash dos eventos
- Buscar valor correto da tabela analysis_events baseado no case_id

Uso:
    python migrate_session_student_hash.py
"""

import sqlite3
import sys
from pathlib import Path

# Adiciona src ao path para imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import structlog

logger = structlog.get_logger()

DB_PATH = Path(__file__).parent / 'src.db'


def migrate_session_student_hash():
    """Atualiza student_hash nas sessões para corresponder aos eventos."""
    
    if not DB_PATH.exists():
        logger.error("Database not found", path=str(DB_PATH))
        print(f"❌ Banco de dados não encontrado: {DB_PATH}")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    try:
        # 1. Contar sessões antes da migração
        cursor.execute("SELECT COUNT(*) FROM sessions")
        total_sessions = cursor.fetchone()[0]
        
        if total_sessions == 0:
            print("✅ Nenhuma sessão encontrada para migrar.")
            return
        
        print(f"📊 Total de sessões no banco: {total_sessions}")
        
        # 2. Buscar todas as sessões com seus case_ids
        cursor.execute("SELECT id, case_id, student_hash FROM sessions")
        sessions = cursor.fetchall()
        
        print(f"\n🔄 Iniciando migração de {len(sessions)} sessões...")
        
        # 3. Para cada sessão, buscar o student_hash correto dos eventos
        migrated = 0
        not_found = 0
        already_correct = 0
        
        for session_id, case_id, old_student_hash in sessions:
            # Buscar student_hash dos eventos com esse case_id
            cursor.execute("""
                SELECT DISTINCT student_hash 
                FROM analysis_events 
                WHERE case_id = ?
                LIMIT 1
            """, (case_id,))
            
            result = cursor.fetchone()
            
            if result:
                correct_student_hash = result[0]
                
                # Verificar se já está correto
                if old_student_hash == correct_student_hash:
                    already_correct += 1
                    continue
                
                # Atualizar sessão com student_hash correto
                cursor.execute("""
                    UPDATE sessions 
                    SET student_hash = ? 
                    WHERE id = ?
                """, (correct_student_hash, session_id))
                
                migrated += 1
                
                if migrated % 10 == 0:
                    print(f"  ✓ Migradas {migrated} sessões...")
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
                    
                    if old_student_hash == correct_student_hash:
                        already_correct += 1
                        continue
                    
                    cursor.execute("""
                        UPDATE sessions 
                        SET student_hash = ? 
                        WHERE id = ?
                    """, (correct_student_hash, session_id))
                    
                    migrated += 1
                    
                    if migrated % 10 == 0:
                        print(f"  ✓ Migradas {migrated} sessões...")
                else:
                    logger.warning("Session without matching events",
                                 session_id=session_id,
                                 case_id=case_id)
                    not_found += 1
        
        # 4. Commit das mudanças
        conn.commit()
        
        # 5. Relatório final
        print("\n" + "="*60)
        print("✅ MIGRAÇÃO CONCLUÍDA")
        print("="*60)
        print(f"Total de sessões:        {total_sessions}")
        print(f"Já estavam corretas:     {already_correct}")
        print(f"Migradas com sucesso:    {migrated}")
        print(f"Não encontradas:         {not_found}")
        print("="*60)
        
        if migrated > 0:
            print("\n🎯 Agora o student_hash nas sessões corresponde aos eventos!")
            print("   JOINs entre 'sessions' e 'events' funcionarão corretamente.")
        
        if not_found > 0:
            print(f"\n⚠️  {not_found} sessões não têm eventos correspondentes.")
            print("   Considere remover essas sessões órfãs.")
        
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
            SELECT COUNT(*) 
            FROM sessions s
            LEFT JOIN analysis_events e ON s.case_id = e.case_id AND s.student_hash = e.student_hash
            WHERE e.case_id IS NULL
        """)
        
        inconsistent = cursor.fetchone()[0]
        
        if inconsistent == 0:
            print("\n✅ VERIFICAÇÃO: Todas as sessões têm student_hash consistente!")
        else:
            print(f"\n⚠️  VERIFICAÇÃO: {inconsistent} sessões ainda inconsistentes")
            
    finally:
        conn.close()


if __name__ == "__main__":
    print("="*60)
    print("🔧 MIGRAÇÃO: Corrigir student_hash em sessões")
    print("="*60)
    print()
    
    migrate_session_student_hash()
    verify_migration()
    
    print("\n✨ Migração finalizada!\n")
