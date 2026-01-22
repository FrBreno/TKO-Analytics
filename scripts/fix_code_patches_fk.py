#!/usr/bin/env python3
"""Corrige restrição de chave estrangeira em `code_patches`.

Problema: a FK em `code_patches.previous_snapshot_id` referenciava `code_snapshots.id`
em vez de `code_patches.id`.

Solução: recriar a tabela com a foreign key correta e restaurar os dados.
"""

import sqlite3
import sys
from pathlib import Path
import structlog

logger = structlog.get_logger()


def backup_table(cursor, table_name: str) -> None:
    """Cria backup dos dados da tabela"""
    cursor.execute(f"CREATE TABLE {table_name}_backup AS SELECT * FROM {table_name}")
    logger.info(f"Backup criado: {table_name}_backup")


def drop_table(cursor, table_name: str) -> None:
    """Remove a tabela (se existir)"""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"Tabela removida: {table_name}")


def create_code_patches_table(cursor) -> None:
    """Cria a tabela `code_patches` com a chave estrangeira CORRETA"""
    cursor.execute("""
        CREATE TABLE code_patches (
            id TEXT PRIMARY KEY,
            case_id TEXT NOT NULL,
            student_hash TEXT NOT NULL,
            student_name TEXT,
            task_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            previous_snapshot_id TEXT,
            patch_text TEXT NOT NULL,
            line_count_delta INTEGER NOT NULL,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (previous_snapshot_id) REFERENCES code_patches(id)
        )
    """)
    logger.info("Tabela code_patches criada com chave estrangeira correta")


def create_indices(cursor) -> None:
    """Cria índices para `code_patches`"""
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patches_case_timestamp ON code_patches(case_id, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patches_snapshot ON code_patches(previous_snapshot_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patches_task ON code_patches(task_id)")
    logger.info("Índices criados")


def restore_data(cursor) -> int:
    """Restaura dados a partir do backup"""
    cursor.execute("""
        INSERT INTO code_patches (
            id, case_id, student_hash, student_name, task_id,
            file_path, timestamp, previous_snapshot_id,
            patch_text, line_count_delta, metadata, created_at
        )
        SELECT 
            id, case_id, student_hash, student_name, task_id,
            file_path, timestamp, previous_snapshot_id,
            patch_text, line_count_delta, metadata,
            COALESCE(created_at, datetime('now'))
        FROM code_patches_backup
    """)
    cursor.execute("SELECT COUNT(*) FROM code_patches")
    count = cursor.fetchone()[0]
    logger.info(f"Restauradas {count} linhas")
    return count


def cleanup_backup(cursor) -> None:
    """Remove a tabela de backup"""
    cursor.execute("DROP TABLE IF EXISTS code_patches_backup")
    logger.info("Tabela de backup removida")


def find_database() -> Path:
    """Localiza arquivo de banco de dados entre candidatos comuns."""
    candidates = [
        Path("src.db"),
        Path("data/src.db"),
        Path(__file__).parent.parent / "src.db",
        Path(__file__).parent.parent / "data" / "src.db"
    ]
    
    for db_path in candidates:
        if db_path.exists():
            return db_path.resolve()
    
    raise FileNotFoundError("Banco de dados não encontrado. Candidatos: " + ", ".join(str(c) for c in candidates))


def main():
    try:
        db_path = find_database()
        print(f"\n✅ Banco de dados encontrado: {db_path}")
        
        print(f"\n⚠️  Este script fará:")
        print(f"  1. Fazer backup da tabela code_patches")
        print(f"  2. Dropar e recriar code_patches com a FK CORRETA")
        print(f"  3. Restaurar dados do backup")
        print(f"  4. Remover tabela de backup")
        
        response = input("\nContinuar? (s/n): ").strip().lower()
        if response != 's':
            print("❌ Cancelado pelo usuário")
            return
        
        logger.info("Iniciando correção da foreign key", db_path=str(db_path))
        
        # Connect
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys = OFF")  # Disable FK checks during migration
        cursor = conn.cursor()
        
        # Check current status
        cursor.execute("PRAGMA foreign_key_list(code_patches)")
        fk_list = cursor.fetchall()
        if fk_list:
            current_ref = f"{fk_list[0][2]}.{fk_list[0][4]}"
            logger.info(f"FK atual: previous_snapshot_id -> {current_ref}")
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='code_patches'")
        if not cursor.fetchone():
            print("⚠️  A tabela code_patches não existe, nada a corrigir")
            return
        
        # Get row count
        cursor.execute("SELECT COUNT(*) FROM code_patches")
        original_count = cursor.fetchone()[0]
        logger.info(f"Tabela possui {original_count} linhas")
        
        # 1. Backup
        print("\n🔄 Etapa 1: Criando backup...")
        backup_table(cursor, "code_patches")
        
        # 2. Drop e recria
        print("🔄 Etapa 2: Recriando tabela com a foreign key correta...")
        drop_table(cursor, "code_patches")
        create_code_patches_table(cursor)
        create_indices(cursor)
        
        # 3. Restaurar dados
        print("🔄 Etapa 3: Restaurando dados...")
        restored_count = restore_data(cursor)
        
        if restored_count != original_count:
            print(f"⚠️  ATENÇÃO: Divergência no número de linhas! Original: {original_count}, Restaurado: {restored_count}")
            response = input("Continuar assim mesmo? (s/n): ").strip().lower()
            if response != 's':
                conn.rollback()
                print("❌ Alterações revertidas")
                return
        
        # 4. Cleanup
        print("🔄 Etapa 4: Limpando...")
        cleanup_backup(cursor)
        
        # Commit
        conn.commit()
        
        # Verifica nova FK
        cursor.execute("PRAGMA foreign_key_list(code_patches)")
        fk_list = cursor.fetchall()
        if fk_list:
            new_ref = f"{fk_list[0][2]}.{fk_list[0][4]}"
            logger.info(f"Nova FK: previous_snapshot_id -> {new_ref}")
            
            if fk_list[0][2] == "code_patches":
                print(f"\n✅ SUCESSO! A foreign key agora referencia corretamente code_patches.id")
            else:
                print(f"\n⚠️  ATENÇÃO: A foreign key ainda referencia {new_ref}")
        
        conn.close()
        
        print(f"\n✅ MIGRAÇÃO CONCLUÍDA!")
        print(f"   Linhas originais: {original_count}")
        print(f"   Linhas restauradas: {restored_count}")
        print(f"\n💡 Próximos passos:")
        print(f"   1. Reimporte seus dados de ANALYSIS")
        print(f"   2. Verifique se os patches foram persistidos corretamente")
        
    except Exception as e:
        logger.error("Migration failed", error=str(e), exc_info=True)
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
