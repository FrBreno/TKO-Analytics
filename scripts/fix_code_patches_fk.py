#!/usr/bin/env python3
"""
Fix code_patches foreign key constraint

Problem: The foreign key in code_patches.previous_snapshot_id
is referencing code_snapshots.id instead of code_patches.id

Solution: Recreate the table with correct foreign key
"""

import sqlite3
import sys
from pathlib import Path
import structlog

logger = structlog.get_logger()


def backup_table(cursor, table_name: str) -> None:
    """Create backup of table data"""
    cursor.execute(f"CREATE TABLE {table_name}_backup AS SELECT * FROM {table_name}")
    logger.info(f"Created backup: {table_name}_backup")


def drop_table(cursor, table_name: str) -> None:
    """Drop table"""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"Dropped table: {table_name}")


def create_code_patches_table(cursor) -> None:
    """Create code_patches table with CORRECT foreign key"""
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
    logger.info("Created code_patches table with correct foreign key")


def create_indices(cursor) -> None:
    """Create indices for code_patches"""
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patches_case_timestamp ON code_patches(case_id, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patches_snapshot ON code_patches(previous_snapshot_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patches_task ON code_patches(task_id)")
    logger.info("Created indices")


def restore_data(cursor) -> int:
    """Restore data from backup"""
    # Explicit column mapping to handle DEFAULT values correctly
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
    logger.info(f"Restored {count} rows")
    return count


def cleanup_backup(cursor) -> None:
    """Drop backup table"""
    cursor.execute("DROP TABLE IF EXISTS code_patches_backup")
    logger.info("Cleaned up backup table")


def find_database() -> Path:
    """Find database file"""
    candidates = [
        Path("src.db"),
        Path("data/src.db"),
        Path(__file__).parent.parent / "src.db",
        Path(__file__).parent.parent / "data" / "src.db"
    ]
    
    for db_path in candidates:
        if db_path.exists():
            return db_path.resolve()
    
    raise FileNotFoundError("Database not found. Candidates: " + ", ".join(str(c) for c in candidates))


def main():
    try:
        # Find database
        db_path = find_database()
        print(f"\n✅ Database found: {db_path}")
        
        # Confirm
        print(f"\n⚠️  This script will:")
        print(f"  1. Backup code_patches table")
        print(f"  2. Drop and recreate code_patches with CORRECT foreign key")
        print(f"  3. Restore data from backup")
        print(f"  4. Clean up backup table")
        
        response = input("\nContinue? (s/n): ").strip().lower()
        if response != 's':
            print("❌ Cancelled by user")
            return
        
        logger.info("Starting foreign key fix", db_path=str(db_path))
        
        # Connect
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys = OFF")  # Disable FK checks during migration
        cursor = conn.cursor()
        
        # Check current status
        cursor.execute("PRAGMA foreign_key_list(code_patches)")
        fk_list = cursor.fetchall()
        if fk_list:
            current_ref = f"{fk_list[0][2]}.{fk_list[0][4]}"
            logger.info(f"Current FK: previous_snapshot_id -> {current_ref}")
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='code_patches'")
        if not cursor.fetchone():
            print("⚠️  code_patches table doesn't exist, nothing to fix")
            return
        
        # Get row count
        cursor.execute("SELECT COUNT(*) FROM code_patches")
        original_count = cursor.fetchone()[0]
        logger.info(f"Table has {original_count} rows")
        
        # 1. Backup
        print("\n🔄 Step 1: Creating backup...")
        backup_table(cursor, "code_patches")
        
        # 2. Drop and recreate
        print("🔄 Step 2: Recreating table with correct foreign key...")
        drop_table(cursor, "code_patches")
        create_code_patches_table(cursor)
        create_indices(cursor)
        
        # 3. Restore data
        print("🔄 Step 3: Restoring data...")
        restored_count = restore_data(cursor)
        
        if restored_count != original_count:
            print(f"⚠️  WARNING: Row count mismatch! Original: {original_count}, Restored: {restored_count}")
            response = input("Continue anyway? (s/n): ").strip().lower()
            if response != 's':
                conn.rollback()
                print("❌ Rolled back changes")
                return
        
        # 4. Cleanup
        print("🔄 Step 4: Cleaning up...")
        cleanup_backup(cursor)
        
        # Commit
        conn.commit()
        
        # Verify new FK
        cursor.execute("PRAGMA foreign_key_list(code_patches)")
        fk_list = cursor.fetchall()
        if fk_list:
            new_ref = f"{fk_list[0][2]}.{fk_list[0][4]}"
            logger.info(f"New FK: previous_snapshot_id -> {new_ref}")
            
            if fk_list[0][2] == "code_patches":
                print(f"\n✅ SUCCESS! Foreign key now correctly references code_patches.id")
            else:
                print(f"\n⚠️  WARNING: Foreign key still references {new_ref}")
        
        conn.close()
        
        print(f"\n✅ MIGRATION COMPLETED!")
        print(f"   Original rows: {original_count}")
        print(f"   Restored rows: {restored_count}")
        print(f"\n💡 Next steps:")
        print(f"   1. Re-import your ANALYSIS data")
        print(f"   2. Verify patches are now persisted correctly")
        
    except Exception as e:
        logger.error("Migration failed", error=str(e), exc_info=True)
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
