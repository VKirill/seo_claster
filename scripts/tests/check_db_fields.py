"""Проверка заполняемости полей в БД"""

import sqlite3
from pathlib import Path

db_path = Path('output/master_queries.db')

if not db_path.exists():
    print(f"БД не найдена: {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Проверяем группу скуд (можно изменить на любую другую)
import sys
group_name = sys.argv[1] if len(sys.argv) > 1 else 'скуд'

cursor.execute('SELECT COUNT(*) FROM master_queries WHERE group_name = ?', (group_name,))
count = cursor.fetchone()[0]

if count == 0:
    print(f"Группа '{group_name}' не найдена в БД")
    conn.close()
    exit(1)

print(f"Найдено запросов: {count}\n")

# Проверяем заполняемость полей
fields = [
    'main_intent',
    'geo_type',
    'geo_country',
    'geo_city',
    'serp_req_id',
    'serp_status',
    'serp_found_docs',
    'serp_titles_with_keyword',
    'serp_docs_with_offers',
    'serp_total_docs',
    'serp_offer_ratio',
    'serp_avg_price',
    'serp_min_price',
    'serp_max_price',
    'serp_median_price',
    'serp_offers_count',
    'serp_offers_with_discount',
    'serp_avg_discount_percent',
]

print("Заполняемость полей:")
print("-" * 80)

for field in fields:
    cursor.execute(f'''
        SELECT 
            COUNT(*) as total,
            COUNT({field}) as filled,
            COUNT(CASE WHEN {field} IS NOT NULL AND {field} != '' AND {field} != 0 THEN 1 END) as non_empty
        FROM master_queries
        WHERE group_name = ?
    ''', (group_name,))
    
    row = cursor.fetchone()
    total, filled, non_empty = row
    percentage = (non_empty / total * 100) if total > 0 else 0
    
    status = "OK" if non_empty > 0 else "EMPTY"
    print(f"{field:35} {non_empty:3}/{total:3} ({percentage:5.1f}%) {status}")

# Примеры записей
print("\n" + "=" * 80)
print("Примеры записей:")
print("=" * 80)

cursor.execute('''
    SELECT 
        keyword,
        main_intent,
        geo_city,
        geo_country,
        serp_req_id,
        serp_status,
        serp_found_docs,
        serp_avg_price,
        serp_offer_ratio
    FROM master_queries
    WHERE group_name = ?
    LIMIT 5
''', (group_name,))

rows = cursor.fetchall()

if rows:
    print(f"\n{'Запрос':<30} {'Интент':<12} {'Город':<12} {'req_id':<12} {'Статус':<12} {'Доков':<10} {'Цена':<10} {'Offer%':<8}")
    print("-" * 120)
    
    for row in rows:
        keyword, intent, city, country, req_id, status, docs, price, ratio = row
        keyword = (keyword[:27] + '...') if keyword and len(keyword) > 30 else (keyword or '')
        intent = str(intent) if intent else ''
        city = str(city) if city else ''
        req_id = (str(req_id)[:10]) if req_id else ''
        status = str(status) if status else ''
        docs = str(docs) if docs else ''
        price = f"{price:.0f}" if price else ''
        ratio = f"{ratio:.1%}" if ratio else ''
        
        print(f"{keyword:<30} {intent:<12} {city:<12} {req_id:<12} {status:<12} {docs:<10} {price:<10} {ratio:<8}")

conn.close()

