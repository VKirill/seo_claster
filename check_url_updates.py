"""
Проверка обновления URL
"""

import sqlite3

conn = sqlite3.connect('output/master_queries.db')
cursor = conn.cursor()

group_name = "николай_чудотворец"

print("🔍 ПРОВЕРКА ОБНОВЛЕНИЯ URL")
print("="*80)

# Запросы с processing и пустыми URL
cursor.execute('''
    SELECT keyword, serp_status, serp_req_id, serp_updated_at
    FROM master_queries 
    WHERE group_name = ? 
      AND serp_status = 'processing'
      AND serp_top_urls = '[]'
    LIMIT 10
''', (group_name,))

print("\n📋 Запросы в статусе 'processing' с пустыми URL:")
print("-"*80)
for keyword, status, req_id, updated in cursor.fetchall():
    keyword_short = keyword[:45] + "..." if len(keyword) > 45 else keyword
    req_id_short = req_id[:15] if req_id else "N/A"
    print(f"{keyword_short:50} | {req_id_short} | {updated}")

# Самые свежие обновления
cursor.execute('''
    SELECT keyword, serp_status, LENGTH(serp_top_urls) as url_len, serp_updated_at
    FROM master_queries 
    WHERE group_name = ?
    ORDER BY serp_updated_at DESC
    LIMIT 10
''', (group_name,))

print("\n🕐 Последние 10 обновлений:")
print("-"*80)
for keyword, status, url_len, updated in cursor.fetchall():
    keyword_short = keyword[:45] + "..." if len(keyword) > 45 else keyword
    url_status = f"{url_len} байт" if url_len > 2 else "ПУСТО []"
    print(f"{keyword_short:50} | {status:10} | {url_status:15} | {updated}")

conn.close()

print("\n" + "="*80)






