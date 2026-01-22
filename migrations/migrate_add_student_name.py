"""
Migração: Adicionar coluna student_name às tabelas.

Este script adiciona a coluna student_name a todas as tabelas relevantes
sem perder dados existentes.
"""

import sqlite3
from pathlib import Path

def migrate_add_student_name(db_path: str = "src.db"):
    """
    Adiciona coluna student_name às tabelas do banco de dados.
    
    Args:
        db_path: Caminho do banco de dados
    """
    db_file = Path(db_path)
    
    if not db_file.exists():
        print(f"❌ Banco de dados não encontrado: {db_path}")
        return False
    
    print(f"🔧 Migrando banco de dados: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Lista de tabelas para adicionar student_name
    tables = [
        'model_events',
        'analysis_events',
        'sessions',
        'code_snapshots',
        'code_patches',
        'metrics',
        'behavioral_patterns'
    ]
    
    success_count = 0
    
    for table in tables:
        try:
            # Verifica se a tabela existe
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not cursor.fetchone():
                print(f"  ⚠️ Tabela {table} não existe, pulando...")
                continue
            
            # Tenta adicionar a coluna
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN student_name TEXT")
            print(f"  ✅ {table}: coluna student_name adicionada")
            success_count += 1
            
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"  ℹ️ {table}: coluna student_name já existe")
            else:
                print(f"  ❌ {table}: erro - {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Migração concluída! {success_count} tabelas atualizadas.")
    return True


if __name__ == '__main__':
    import sys
    
    db_path = sys.argv[1] if len(sys.argv) > 1 else "src.db"
    
    print("=" * 60)
    print("MIGRAÇÃO: Adicionar coluna student_name")
    print("=" * 60)
    
    success = migrate_add_student_name(db_path)
    
    if success:
        print("\n💡 Você pode agora reimportar os dados do TKO.")
    else:
        print("\n❌ Migração falhou.")
        sys.exit(1)
