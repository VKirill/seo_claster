"""Быстрая проверка наличия запросов в БД"""
import sqlite3
from pathlib import Path

db_path = Path("output/master_queries.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Проверяем группы
cursor.execute("SELECT DISTINCT group_name FROM master_queries")
groups = [r[0] for r in cursor.fetchall()]
print("Группы в БД:", groups)

# Проверяем наличие "скуд обои"
cursor.execute("SELECT keyword, group_name FROM master_queries WHERE keyword LIKE '%скуд обои%'")
results = cursor.fetchall()
print("\nЗапросы с 'скуд обои':")
for r in results:
    print(f"  {r[0]} | группа: {r[1]}")

# Проверяем сколько запросов в группе "скуд"
cursor.execute("SELECT COUNT(*) FROM master_queries WHERE group_name = 'скуд'")
count = cursor.fetchone()[0]
print(f"\nВсего запросов в группе 'скуд': {count}")

# Проверяем наличие SERP данных
cursor.execute("SELECT COUNT(*) FROM master_queries WHERE group_name = 'скуд' AND serp_top_urls IS NOT NULL AND serp_top_urls != ''")
serp_count = cursor.fetchone()[0]
print(f"Запросов с SERP данными: {serp_count}")

conn.close()

