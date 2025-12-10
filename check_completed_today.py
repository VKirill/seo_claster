"""
Проверка обновлений completed за сегодня
"""

import sqlite3
from datetime import datetime

conn = sqlite3.connect('output/master_queries.db')
cursor = conn.cursor()

group_name = "николай_чудотворец"

print("="*80)
print("📊 ОБНОВЛЕНИЯ COMPLETED ЗА СЕГОДНЯ")
print("="*80)

# Completed за сегодня  
cursor.execute('''
    SELECT 
        DATE(serp_updated_at) as date,
        COUNT(*) as count
    FROM master_queries 
    WHERE group_name = ? 
      AND serp_status = 'completed'
      AND DATE(serp_updated_at) = DATE('now')
    GROUP BY DATE(serp_updated_at)
''', (group_name,))

today = cursor.fetchall()

if today:
    for date, count in today:
        print(f"\n✅ Сегодня ({date}): {count} запросов перешли в 'completed'")
else:
    print(f"\n❌ Сегодня НЕТ обновлений в 'completed'!")
    print(f"   Все 'completed' записи - старые (до сегодня)")

# Последние completed
cursor.execute('''
    SELECT keyword, serp_updated_at, LENGTH(serp_top_urls) as url_len
    FROM master_queries 
    WHERE group_name = ? 
      AND serp_status = 'completed'
      AND LENGTH(serp_top_urls) > 2
    ORDER BY serp_updated_at DESC
    LIMIT 5
''', (group_name,))

print(f"\n🕐 Последние 5 'completed' с URL:")
print("-"*80)
for keyword, updated, url_len in cursor.fetchall():
    keyword_short = keyword[:50] + "..." if len(keyword) > 50 else keyword
    print(f"{keyword_short:55} | {updated} | {url_len} байт")

conn.close()

print("\n" + "="*80)


