#!/usr/bin/env python3
"""Conta `student_hash` distintos na tabela `analysis_events` de um banco SQLite.

Uso:
    python count_analysis_students.py --db path/to/src.db

Se `--db` não for fornecido, usa `./src.db` por padrão.
"""
import argparse
import sqlite3
import sys
import os


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', default='src.db', help='Path to SQLite DB (default: src.db)')
    args = p.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f'Database file not found: {db_path}', file=sys.stderr)
        sys.exit(2)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # check if table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ('analysis_events',))
    row = cur.fetchone()
    if not row:
        print('Table `analysis_events` not found in database.', file=sys.stderr)
        sys.exit(3)

    try:
        cur.execute('SELECT COUNT(DISTINCT student_hash) FROM analysis_events')
        cnt = cur.fetchone()[0]
        print(cnt)
    except Exception as e:
        print('Query failed:', e, file=sys.stderr)
        sys.exit(4)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
