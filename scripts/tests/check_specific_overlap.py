"""
Проверка конкретных запросов на пересечение URL
"""

import json
import sqlite3
from pathlib import Path
from typing import List

DB_PATH = Path("output/master_queries.db")
GROUP_NAME = "скуд"


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


def calculate_url_overlap(urls1: List[str], urls2: List[str], top_n: int = 20) -> tuple[int, set]:
    set1 = set(urls1[:top_n])
    set2 = set(urls2[:top_n])
    common = set1 & set2
    return len(common), common


def main():
    query1 = "скуд в офис"
    query2 = "система скуд в офис"
    
    print("=" * 80)
    print(f"ПРОВЕРКА ПЕРЕСЕЧЕНИЯ URL МЕЖДУ ЗАПРОСАМИ")
    print("=" * 80)
    print(f"\nЗапрос 1: '{query1}'")
    print(f"Запрос 2: '{query2}'")
    print()
    
    # Загружаем URL
    urls1 = get_query_urls(DB_PATH, GROUP_NAME, query1)
    urls2 = get_query_urls(DB_PATH, GROUP_NAME, query2)
    
    print(f"📥 URL для '{query1}': {len(urls1)}")
    print(f"📥 URL для '{query2}': {len(urls2)}")
    
    if not urls1:
        print(f"\n❌ Нет данных для '{query1}' в БД")
        return
    
    if not urls2:
        print(f"\n❌ Нет данных для '{query2}' в БД")
        return
    
    # Вычисляем пересечение
    overlap_count, common_urls = calculate_url_overlap(urls1, urls2, top_n=20)
    
    print(f"\n{'=' * 80}")
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    print(f"\n✅ Общих URL: {overlap_count} из 20")
    print(f"   Процент совпадения: {overlap_count / 20 * 100:.1f}%")
    
    if common_urls:
        print(f"\n📋 Общие URL (первые 20):")
        for i, url in enumerate(sorted(common_urls)[:20], 1):
            print(f"   {i}. {url}")
    
    print(f"\n📋 URL только в '{query1}' (первые 10):")
    only1 = set(urls1[:20]) - common_urls
    for i, url in enumerate(sorted(only1)[:10], 1):
        print(f"   {i}. {url}")
    
    print(f"\n📋 URL только в '{query2}' (первые 10):")
    only2 = set(urls2[:20]) - common_urls
    for i, url in enumerate(sorted(only2)[:10], 1):
        print(f"   {i}. {url}")
    
    # Проверяем все URL для детального анализа
    print(f"\n{'=' * 80}")
    print("ДЕТАЛЬНЫЙ АНАЛИЗ")
    print("=" * 80)
    print(f"\nВсе URL для '{query1}' (первые 20):")
    for i, url in enumerate(urls1[:20], 1):
        marker = "✅" if url in common_urls else "  "
        print(f"   {marker} {i}. {url}")
    
    print(f"\nВсе URL для '{query2}' (первые 20):")
    for i, url in enumerate(urls2[:20], 1):
        marker = "✅" if url in common_urls else "  "
        print(f"   {marker} {i}. {url}")


if __name__ == "__main__":
    main()

