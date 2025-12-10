"""Анализ ошибок в failed запросах"""

import sqlite3
from collections import Counter

conn = sqlite3.connect('output/master_queries.db')
cursor = conn.cursor()

group_name = "николай_чудотворец"

print("="*80)
print("❌ АНАЛИЗ FAILED ЗАПРОСОВ")
print("="*80)

# Группировка по типам ошибок
cursor.execute('''
    SELECT serp_error_message, COUNT(*) as count
    FROM master_queries 
    WHERE group_name = ? AND serp_status = 'failed'
    GROUP BY serp_error_message
    ORDER BY count DESC
    LIMIT 20
''', (group_name,))

errors = cursor.fetchall()

print(f"\n📊 Топ ошибок:")
print("-"*80)

for error_msg, count in errors:
    error_short = (error_msg or "NULL")[:60]
    print(f"  {count:5}x | {error_short}")

# Проверяем есть ли failed с req_id (можно попробовать получить)
cursor.execute('''
    SELECT COUNT(*)
    FROM master_queries 
    WHERE group_name = ? 
      AND serp_status = 'failed'
      AND serp_req_id IS NOT NULL
''', (group_name,))

failed_with_reqid = cursor.fetchone()[0]

print()
print("-"*80)
print(f"  Failed с req_id: {failed_with_reqid}")
print(f"  Failed БЕЗ req_id: {len([e for e in errors]) - failed_with_reqid}")

# Примеры failed с req_id
if failed_with_reqid > 0:
    cursor.execute('''
        SELECT keyword, serp_req_id, serp_error_message
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'failed'
          AND serp_req_id IS NOT NULL
        LIMIT 5
    ''', (group_name,))
    
    print(f"\n📋 Примеры failed С req_id (можно попробовать получить):")
    print("-"*80)
    for keyword, req_id, error_msg in cursor.fetchall():
        keyword_short = keyword[:40] + "..." if len(keyword) > 40 else keyword
        error_short = (error_msg or "N/A")[:30]
        print(f"  {keyword_short:45} | {req_id[:15]} | {error_short}")

print("\n" + "="*80)

conn.close()


