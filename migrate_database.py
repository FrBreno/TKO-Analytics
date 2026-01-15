"""
Script de migração para separar tabela events em model_events e analysis_events.

Migra dados existentes do schema antigo (tabela events única) para o novo schema
(tabelas model_events e analysis_events separadas).
"""

import sqlite3
import sys
from pathlib import Path
import structlog

logger = structlog.get_logger()


def migrate_database(db_path: str = "src.db") -> dict:
    """
    Migra banco de dados existente para o novo schema com tabelas separadas.
    
    Args:
        db_path: Caminho do banco de dados
        
    Returns:
        Dict com estatísticas da migração
    """
    db_path = Path(db_path).resolve()
    
    if not db_path.exists():
        logger.error("Database not found", path=str(db_path))
        return {
            'success': False,
            'error': f'Database not found: {db_path}'
        }
    
    logger.info("Starting database migration", db_path=str(db_path))
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # Verificar se tabela events antiga existe
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='events'
        """)
        
        if not cursor.fetchone():
            logger.info("Old 'events' table not found - assuming fresh database")
            return {
                'success': True,
                'message': 'Database already migrated or is fresh (no events table found)',
                'model_migrated': 0,
                'analysis_migrated': 0
            }
        
        # Verificar se já tem as novas tabelas
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='model_events'
        """)
        has_new_tables = cursor.fetchone() is not None
        
        if not has_new_tables:
            logger.info("Creating new tables (model_events, analysis_events)")
            from src.etl.init_db import init_database
            init_database(str(db_path))
        
        # Contar eventos por role
        cursor.execute("SELECT COUNT(*) FROM events WHERE dataset_role = 'model'")
        model_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM events WHERE dataset_role = 'analysis'")
        analysis_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM events WHERE dataset_role IS NULL OR dataset_role = ''")
        null_count = cursor.fetchone()[0]
        
        logger.info("Events inventory",
                   model=model_count,
                   analysis=analysis_count,
                   null_role=null_count)
        
        # Migrar eventos MODEL
        logger.info("Migrating MODEL events")
        cursor.execute("""
            INSERT OR IGNORE INTO model_events (
                id, case_id, student_hash, task_id, activity,
                event_type, timestamp, duration_seconds, session_id, metadata, created_at
            )
            SELECT 
                id, case_id, student_hash, task_id, activity,
                event_type, timestamp, duration_seconds, session_id, metadata, created_at
            FROM events
            WHERE dataset_role = 'model'
        """)
        model_migrated = cursor.rowcount
        
        # Migrar eventos ANALYSIS
        logger.info("Migrating ANALYSIS events")
        cursor.execute("""
            INSERT OR IGNORE INTO analysis_events (
                id, case_id, student_hash, task_id, activity,
                event_type, timestamp, duration_seconds, session_id, metadata, created_at
            )
            SELECT 
                id, case_id, student_hash, task_id, activity,
                event_type, timestamp, duration_seconds, session_id, metadata, created_at
            FROM events
            WHERE dataset_role = 'analysis' OR dataset_role IS NULL OR dataset_role = ''
        """)
        analysis_migrated = cursor.rowcount
        
        conn.commit()
        
        logger.info("Migration completed",
                   model_migrated=model_migrated,
                   analysis_migrated=analysis_migrated)
        
        # Verificar migração
        cursor.execute("SELECT COUNT(*) FROM model_events")
        model_final = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM analysis_events")
        analysis_final = cursor.fetchone()[0]
        
        logger.info("Migration verification",
                   model_events=model_final,
                   analysis_events=analysis_final)
        
        # OPCIONAL: Remover tabela antiga (comentado por segurança)
        # cursor.execute("DROP TABLE events")
        # conn.commit()
        # logger.info("Old 'events' table dropped")
        
        return {
            'success': True,
            'message': f'Migration successful: {model_migrated} MODEL + {analysis_migrated} ANALYSIS events migrated',
            'model_migrated': model_migrated,
            'analysis_migrated': analysis_migrated,
            'model_final_count': model_final,
            'analysis_final_count': analysis_final
        }
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Migration failed", error=str(e), exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        conn.close()


def drop_old_events_table(db_path: str = "src.db") -> dict:
    """
    Remove a tabela 'events' antiga após confirmar migração bem-sucedida.
    
    CUIDADO: Esta operação é irreversível!
    
    Args:
        db_path: Caminho do banco de dados
        
    Returns:
        Dict com resultado da operação
    """
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    
    try:
        cursor = conn.cursor()
        
        # Verificar se existe
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='events'
        """)
        
        if not cursor.fetchone():
            return {
                'success': True,
                'message': 'Table "events" already dropped or never existed'
            }
        
        # Contar eventos na tabela antiga
        cursor.execute("SELECT COUNT(*) FROM events")
        old_count = cursor.fetchone()[0]
        
        # Dropar
        cursor.execute("DROP TABLE events")
        conn.commit()
        
        logger.info("Old events table dropped", rows_removed=old_count)
        
        return {
            'success': True,
            'message': f'Old "events" table dropped ({old_count} rows removed)',
            'rows_removed': old_count
        }
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Failed to drop old table", error=str(e))
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate TKO Analytics database to separated tables")
    parser.add_argument('--db', default='src.db', help='Database path (default: src.db)')
    parser.add_argument('--drop-old', action='store_true', 
                       help='Drop old events table after migration (IRREVERSIBLE!)')
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("TKO ANALYTICS - DATABASE MIGRATION")
    print("="*60)
    
    # Migrar
    result = migrate_database(args.db)
    
    if result['success']:
        print(f"\n✓ {result['message']}")
        if 'model_migrated' in result:
            print(f"  - MODEL events: {result['model_final_count']}")
            print(f"  - ANALYSIS events: {result['analysis_final_count']}")
        
        # Dropar tabela antiga se solicitado
        if args.drop_old:
            print("\n⚠️  Dropping old 'events' table...")
            drop_result = drop_old_events_table(args.db)
            if drop_result['success']:
                print(f"✓ {drop_result['message']}")
            else:
                print(f"✗ Failed: {drop_result['error']}")
                sys.exit(1)
        else:
            print("\n💡 Old 'events' table kept. Run with --drop-old to remove it.")
        
        sys.exit(0)
    else:
        print(f"\n✗ Migration failed: {result['error']}")
        sys.exit(1)
