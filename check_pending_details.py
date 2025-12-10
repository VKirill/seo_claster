"""Детальная проверка pending запросов"""

import sqlite3

conn = sqlite3.connect('output/master_queries.db')
cursor = conn.cursor()

group_name = "николай_чудотворец"

print("="*80)
print("🔍 ДЕТАЛИ PENDING ЗАПРОСОВ")
print("="*80)

# Всего pending
cursor.execute('''
    SELECT COUNT(*), COUNT(serp_req_id)
    FROM master_queries 
    WHERE group_name = ? AND serp_status = 'pending'
''', (group_name,))

total, with_reqid = cursor.fetchone()

print(f"\n📋 Pending всего: {total}")
print(f"   С req_id: {with_reqid}")
print(f"   БЕЗ req_id: {total - with_reqid}")

# Примеры с req_id
if with_reqid > 0:
    cursor.execute('''
        SELECT keyword, serp_req_id, serp_updated_at
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'pending'
          AND serp_req_id IS NOT NULL
        LIMIT 5
    ''', (group_name,))
    
    print(f"\n📋 Примеры pending С req_id:")
    print("-"*80)
    for keyword, req_id, updated in cursor.fetchall():
        keyword_short = keyword[:45] + "..." if len(keyword) > 45 else keyword
        print(f"  {keyword_short:50} | {req_id[:15]} | {updated}")

# Примеры без req_id
if total - with_reqid > 0:
    cursor.execute('''
        SELECT keyword, serp_error_message, serp_updated_at
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'pending'
          AND (serp_req_id IS NULL OR serp_req_id = '')
        LIMIT 5
    ''', (group_name,))
    
    print(f"\n📋 Примеры pending БЕЗ req_id:")
    print("-"*80)
    for keyword, error_msg, updated in cursor.fetchall():
        keyword_short = keyword[:45] + "..." if len(keyword) > 45 else keyword
        error_short = (error_msg or "нет сообщения")[:30]
        print(f"  {keyword_short:50} | {error_short:32} | {updated}")

print("\n" + "="*80)

conn.close()


