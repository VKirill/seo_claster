"""
Проверка структуры serp_data.db

⚠️ DEPRECATED: Этот скрипт использует устаревшую БД serp_data.db
Все данные теперь хранятся в master_queries.db (MasterQueryDatabase)
"""

import sys

print("⚠️  ВНИМАНИЕ: Этот скрипт устарел!")
print("   serp_data.db больше не используется.")
print("   Все данные теперь в master_queries.db")
sys.exit(1)

"""Проверка структуры serp_data.db"""
import sqlite3

conn = sqlite3.connect('serp_data.db')
c = conn.cursor()

print("=" * 60)
print("ТАБЛИЦЫ В serp_data.db:")
print("=" * 60)

c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = c.fetchall()

for table in tables:
    table_name = table[0]
    print(f"\n📊 {table_name}")
    
    c.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = c.fetchone()[0]
    print(f"   Записей: {count}")

conn.close()

