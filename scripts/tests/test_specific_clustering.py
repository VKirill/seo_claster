"""
Тест конкретных запросов "скуд в офис" и "система скуд в офис"
"""

import json
import sqlite3
from pathlib import Path
from typing import List, Dict

DB_PATH = Path("output/master_queries.db")
GROUP_NAME = "скуд"
MIN_COMMON_URLS = 7

QUERIES = [
    "скуд в офис",
    "система скуд в офис",
]


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.", "")
    url = url.split("?")[0].split("#")[0]
    return url.rstrip("/").lower()


def extract_urls_from_json(serp_top_urls_json: str) -> List[str]:
    if not serp_top_urls_json:
        return []
    try:
        data = json.loads(serp_top_urls_json)
        if isinstance(data, list):
            urls = []
            for item in data:
                if isinstance(item, dict):
                    url = item.get('url', '')
                elif isinstance(item, str):
                    url = item
                else:
                    continue
                if url:
                    urls.append(normalize_url(url))
            return urls
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def get_query_urls(db_path: Path, group_name: str, query: str) -> List[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT serp_top_urls
        FROM master_queries
        WHERE group_name = ? AND keyword = ?
    ''', (group_name, query))
    row = cursor.fetchone()
    conn.close()
    if row and row[0]:
        return extract_urls_from_json(row[0])
    return []


def calculate_url_overlap(urls1: List[str], urls2: List[str], top_n: int = 20) -> int:
    set1 = set(urls1[:top_n])
    set2 = set(urls2[:top_n])
    return len(set1 & set2)


def can_add_to_cluster(
    query: str,
    cluster_queries: List[str],
    query_urls_dict: Dict[str, List[str]],
    min_common_urls: int
) -> bool:
    if not cluster_queries:
        return True
    
    query_urls = query_urls_dict.get(query, [])
    
    for cluster_query in cluster_queries:
        cluster_query_urls = query_urls_dict.get(cluster_query, [])
        overlap = calculate_url_overlap(query_urls, cluster_query_urls, top_n=20)
        
        if overlap < min_common_urls:
            return False
    
    return True


def main():
    print("=" * 80)
    print("ТЕСТ: 'скуд в офис' и 'система скуд в офис'")
    print("=" * 80)
    
    # Загружаем URL
    query_urls_dict = {}
    for query in QUERIES:
        urls = get_query_urls(DB_PATH, GROUP_NAME, query)
        if urls:
            query_urls_dict[query] = urls
            print(f"\n✓ {query}: {len(urls)} URL")
    
    if len(query_urls_dict) != 2:
        print("\n❌ Не все запросы найдены в БД")
        return
    
    query1 = "скуд в офис"
    query2 = "система скуд в офис"
    
    # Проверяем связь между ними
    overlap = calculate_url_overlap(query_urls_dict[query1], query_urls_dict[query2], top_n=20)
    print(f"\n📊 Связь между запросами: {overlap} общих URL")
    print(f"   Порог для сильной связи: {MIN_COMMON_URLS * 2} (min * 2)")
    print(f"   Является ли сильной связью: {overlap >= MIN_COMMON_URLS * 2}")
    
    # Симулируем кластеризацию
    print(f"\n{'=' * 80}")
    print("СИМУЛЯЦИЯ КЛАСТЕРИЗАЦИИ")
    print("=" * 80)
    
    # Вариант 1: "система скуд в офис" обрабатывается первым
    print(f"\n📌 Вариант 1: '{query2}' обрабатывается первым")
    print(f"   1. '{query2}' создает кластер 1")
    print(f"   2. '{query1}' находит '{query2}' с {overlap} общими URL")
    print(f"   3. Проверка: overlap ({overlap}) >= threshold ({MIN_COMMON_URLS * 2})? {overlap >= MIN_COMMON_URLS * 2}")
    
    if overlap >= MIN_COMMON_URLS * 2:
        print(f"   ✅ ДА - создается отдельный кластер только с этими двумя запросами")
        print(f"   ✅ Результат: Кластер 1 = ['{query1}', '{query2}']")
    else:
        print(f"   ❌ НЕТ - проверяется можно ли добавить в существующий кластер")
        can_add = can_add_to_cluster(query1, [query2], query_urls_dict, MIN_COMMON_URLS)
        print(f"   ✅ Можно добавить: {can_add}")
        if can_add:
            print(f"   ✅ Результат: Кластер 1 = ['{query1}', '{query2}']")
        else:
            print(f"   ❌ Результат: Кластер 1 = ['{query2}'], Кластер 2 = ['{query1}']")
    
    # Вариант 2: "скуд в офис" обрабатывается первым
    print(f"\n📌 Вариант 2: '{query1}' обрабатывается первым")
    print(f"   1. '{query1}' создает кластер 1")
    print(f"   2. '{query2}' находит '{query1}' с {overlap} общими URL")
    print(f"   3. Проверка: overlap ({overlap}) >= threshold ({MIN_COMMON_URLS * 2})? {overlap >= MIN_COMMON_URLS * 2}")
    
    if overlap >= MIN_COMMON_URLS * 2:
        print(f"   ✅ ДА - создается отдельный кластер только с этими двумя запросами")
        print(f"   ✅ Результат: Кластер 1 = ['{query1}', '{query2}']")
    else:
        print(f"   ❌ НЕТ - проверяется можно ли добавить в существующий кластер")
        can_add = can_add_to_cluster(query2, [query1], query_urls_dict, MIN_COMMON_URLS)
        print(f"   ✅ Можно добавить: {can_add}")
        if can_add:
            print(f"   ✅ Результат: Кластер 1 = ['{query1}', '{query2}']")
        else:
            print(f"   ❌ Результат: Кластер 1 = ['{query1}'], Кластер 2 = ['{query2}']")


if __name__ == "__main__":
    main()

