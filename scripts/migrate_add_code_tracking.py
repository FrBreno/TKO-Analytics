"""
Script de migração para adicionar suporte a code tracking em bancos existentes.

Adiciona:
- Campo metadata em code_snapshots
- Campo metadata em code_patches
- Novos índices para task_id

Uso:
    python scripts/migrate_add_code_tracking.py
"""

import os
import sys
import sqlite3
from pathlib import Path

# Adiciona src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ]
)

logger = structlog.get_logger()


def check_column_exists(cursor, table_name: str, column_name: str) -> bool:
    """Verifica se coluna existe na tabela."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def check_index_exists(cursor, index_name: str) -> bool:
    """Verifica se índice existe."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,))
    return cursor.fetchone() is not None


def migrate_database(db_path: str):
    """
    Aplica migrações necessárias para suporte a code tracking.
    
    Args:
        db_path: Caminho do banco de dados
    """
    db_path = Path(db_path).resolve()
    
    if not db_path.exists():
        logger.error("Database not found", db_path=str(db_path))
        return False
    
    logger.info("Starting migration", db_path=str(db_path))
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    migrations_applied = []
    
    try:
        # Migração 1: Adicionar metadata em code_snapshots
        if not check_column_exists(cursor, 'code_snapshots', 'metadata'):
            logger.info("Adding column: code_snapshots.metadata")
            cursor.execute("ALTER TABLE code_snapshots ADD COLUMN metadata TEXT")
            migrations_applied.append("code_snapshots.metadata")
        else:
            logger.info("Column already exists: code_snapshots.metadata")
        
        # Migração 2: Adicionar metadata em code_patches
        if not check_column_exists(cursor, 'code_patches', 'metadata'):
            logger.info("Adding column: code_patches.metadata")
            cursor.execute("ALTER TABLE code_patches ADD COLUMN metadata TEXT")
            migrations_applied.append("code_patches.metadata")
        else:
            logger.info("Column already exists: code_patches.metadata")
        
        # Migração 3: Adicionar índice task_id em code_snapshots
        if not check_index_exists(cursor, 'idx_snapshots_task'):
            logger.info("Creating index: idx_snapshots_task")
            cursor.execute("CREATE INDEX idx_snapshots_task ON code_snapshots(task_id)")
            migrations_applied.append("idx_snapshots_task")
        else:
            logger.info("Index already exists: idx_snapshots_task")
        
        # Migração 4: Adicionar índice task_id em code_patches
        if not check_index_exists(cursor, 'idx_patches_task'):
            logger.info("Creating index: idx_patches_task")
            cursor.execute("CREATE INDEX idx_patches_task ON code_patches(task_id)")
            migrations_applied.append("idx_patches_task")
        else:
            logger.info("Index already exists: idx_patches_task")
        
        # Migração 5: Atualizar compression default para 'none'
        # SQLite não suporta ALTER COLUMN, então verificamos manualmente
        cursor.execute("PRAGMA table_info(code_snapshots)")
        for row in cursor.fetchall():
            if row[1] == 'compression':
                default_value = row[4]
                if default_value and 'gzip' in str(default_value):
                    logger.warning("Default compression is 'gzip', cannot change to 'none' in existing table")
                    logger.info("Future snapshots will use 'none', existing data unaffected")
                break
        
        conn.commit()
        
        if migrations_applied:
            logger.info("Migration complete", 
                       changes=len(migrations_applied),
                       applied=migrations_applied)
        else:
            logger.info("No migrations needed - database already up to date")
        
        return True
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Migration failed", error=str(e))
        return False
    finally:
        conn.close()


def main():
    print("\n" + "="*70)
    print("MIGRAÇÃO: Suporte a Code Tracking")
    print("="*70 + "\n")
    
    # Usar banco configurado ou padrão (buscar na raiz do projeto)
    default_paths = [
        'src.db',
        'data/src.db',
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src.db')
    ]
    
    db_path = os.getenv('TKO_DB_PATH')
    
    if not db_path:
        # Tentar encontrar banco automaticamente
        for path in default_paths:
            if Path(path).exists():
                db_path = path
                print(f"✅ Banco encontrado: {path}")
                break
    
    if not db_path or not Path(db_path).exists():
        print(f"❌ Erro: Banco de dados não encontrado")
        print(f"\nLocais verificados:")
        for path in default_paths:
            print(f"  - {path}")
        print("\nDefina TKO_DB_PATH ou execute de dentro do projeto")
        return 1
    
    print(f"Banco de dados: {db_path}\n")
    
    # Confirmar antes de migrar
    response = input("Deseja aplicar as migrações? [s/N]: ").strip().lower()
    if response not in ['s', 'sim', 'y', 'yes']:
        print("\n⚠️  Migração cancelada pelo usuário")
        return 0
    
    print()
    
    success = migrate_database(db_path)
    
    print("\n" + "="*70)
    if success:
        print("✅ MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*70)
        print("\nO banco de dados está pronto para code tracking.")
        print("Você pode importar dados ANALYSIS normalmente agora.\n")
        return 0
    else:
        print("❌ MIGRAÇÃO FALHOU!")
        print("="*70)
        print("\nVerifique os logs acima para detalhes do erro.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
