import sqlite3

conn = sqlite3.connect('src.db')
cursor = conn.cursor()

# Lista todas as tabelas
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()

print("TABELAS:")
for table in tables:
    print(f"  - {table[0]}")

print("\n" + "="*80)

# Mostra schema de cada tabela
for table in tables:
    table_name = table[0]
    print(f"\nTABELA: {table_name}")
    print("-" * 80)
    
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    schema = cursor.fetchone()[0]
    print(schema)
    
    # Mostra exemplos
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 2")
    rows = cursor.fetchall()
    if rows:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        print(f"\nCOLUNAS: {', '.join(columns)}")
        print(f"\nEXEMPLO (primeira linha):")
        if rows:
            for col, val in zip(columns, rows[0]):
                print(f"  {col}: {val}")

conn.close()
