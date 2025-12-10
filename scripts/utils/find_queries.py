"""Поиск запросов в БД"""
import sqlite3
import sys

keyword = sys.argv[1] if len(sys.argv) > 1 else "кабель"

conn = sqlite3.connect('output/serp_data.db')
cursor = conn.cursor()

cursor.execute("""
    SELECT query, commercial_domains, info_domains, found_docs
    FROM serp_results
    WHERE LOWER(query) LIKE ?
    ORDER BY created_at DESC
    LIMIT 10
""", (f"%{keyword.lower()}%",))

results = cursor.fetchall()

if results:
    print(f"Найдено {len(results)} запросов со словом '{keyword}':\n")
    for query, comm, info, docs in results:
        print(f"📌 {query}")
        print(f"   Комм: {comm}, Инфо: {info}, Docs: {docs:,}\n")
else:
    print(f"Нет запросов со словом '{keyword}'")

conn.close()

