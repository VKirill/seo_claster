"""
Тестовый скрипт для проверки пересечения URL между запросами
Проверяет почему "скуд обои" попал в кластер с запросами про СКУД
"""

import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Set, Tuple
from collections import defaultdict

# Список запросов из кластера
QUERIES = [
    "система скуд",
    "контроль доступа скуд",
    "скуд система контроля",
    "система контроля доступа скуд",
    "скуд система контроля и управления доступом",
    "оборудование скуд",
    "скуд в офис",
    "скуд охранная сигнализация",
    "системы безопасности скуд",
    "скуд накладной",
    "современный скуд",
    "система скуд для дверей",
    "автоматизированная скуд",
    "оснащение скуд",
    "система скуд в офис",
    "скуд сова",
    "станция скуд",
    "система учета контроля скуд",
    "система контроля учета доступа скуд",
    "система контроля удаленного доступа скуд",
    "системы скуд для предприятий",
    "ростаб скуд",
    "электроника скуд",
    "ред скуд",
    "скуд обои",  # Проблемный запрос
    "скуд рим",
]

# Путь к базе данных
DB_PATH = Path("output/master_queries.db")
GROUP_NAME = "скуд"  # Название группы (без подчеркивания)


def normalize_url(url: str) -> str:
    """Нормализует URL для сравнения"""
    if not url:
        return ""
    # Убираем протокол
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.", "")
    # Убираем параметры и якоря
    url = url.split("?")[0].split("#")[0]
    # Убираем trailing slash
    url = url.rstrip("/")
    return url.lower()


def extract_urls_from_json(serp_top_urls_json: str) -> List[str]:
    """Извлекает URL из JSON строки"""
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
    """Получает URL для запроса из базы данных"""
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


def calculate_url_overlap(urls1: List[str], urls2: List[str], top_n: int = 20) -> Tuple[int, Set[str]]:
    """Вычисляет пересечение URL между двумя запросами"""
    set1 = set(urls1[:top_n])
    set2 = set(urls2[:top_n])
    common = set1 & set2
    return len(common), common


def main():
    print("=" * 80)
    print("ПРОВЕРКА ПЕРЕСЕЧЕНИЯ URL МЕЖДУ ЗАПРОСАМИ")
    print("=" * 80)
    print(f"\nБаза данных: {DB_PATH}")
    print(f"Группа: {GROUP_NAME}")
    print(f"Всего запросов для проверки: {len(QUERIES)}")
    print()
    
    if not DB_PATH.exists():
        print(f"❌ Ошибка: База данных не найдена: {DB_PATH}")
        return
    
    # Загружаем URL для всех запросов
    print("📥 Загрузка URL из базы данных...")
    query_urls_dict = {}
    queries_without_data = []
    
    for query in QUERIES:
        urls = get_query_urls(DB_PATH, GROUP_NAME, query)
        if urls:
            query_urls_dict[query] = urls
            print(f"  ✓ {query}: {len(urls)} URL")
        else:
            queries_without_data.append(query)
            print(f"  ⚠️  {query}: нет данных в БД")
    
    print(f"\n✓ Загружено: {len(query_urls_dict)} запросов с данными")
    if queries_without_data:
        print(f"⚠️  Без данных: {len(queries_without_data)} запросов")
        for q in queries_without_data:
            print(f"     - {q}")
    
    if "скуд обои" not in query_urls_dict:
        print("\n❌ Ошибка: Запрос 'скуд обои' не найден в базе данных!")
        return
    
    # Проверяем пересечение "скуд обои" с остальными запросами
    target_query = "скуд обои"
    target_urls = query_urls_dict[target_query]
    
    print("\n" + "=" * 80)
    print(f"АНАЛИЗ ПЕРЕСЕЧЕНИЯ: '{target_query}' с остальными запросами")
    print("=" * 80)
    print(f"\nURL для '{target_query}': {len(target_urls)}")
    if target_urls:
        print("Первые 10 URL:")
        for i, url in enumerate(target_urls[:10], 1):
            print(f"  {i}. {url}")
    
    # Порог для кластеризации (из конфига)
    MIN_COMMON_URLS = 7  # Изменено с 8 на 7 для теста
    
    print(f"\nПорог для кластеризации: {MIN_COMMON_URLS} общих URL")
    print(f"\n{'Запрос':<50} {'Общих URL':<12} {'Статус':<15} {'Общие URL'}")
    print("-" * 120)
    
    results = []
    for query in QUERIES:
        if query == target_query:
            continue
        
        if query not in query_urls_dict:
            print(f"{query:<50} {'N/A':<12} {'Нет данных':<15}")
            continue
        
        urls = query_urls_dict[query]
        common_count, common_urls = calculate_url_overlap(target_urls, urls, top_n=20)
        
        status = "✅ В кластере" if common_count >= MIN_COMMON_URLS else "❌ Не в кластере"
        
        results.append({
            'query': query,
            'common_count': common_count,
            'common_urls': common_urls,
            'status': status
        })
        
        # Показываем первые 3 общих URL
        common_preview = ", ".join(list(common_urls)[:3]) if common_urls else "-"
        if len(common_urls) > 3:
            common_preview += f" ... (+{len(common_urls) - 3} еще)"
        
        print(f"{query:<50} {common_count:<12} {status:<15} {common_preview}")
    
    # Статистика
    print("\n" + "=" * 80)
    print("СТАТИСТИКА")
    print("=" * 80)
    
    in_cluster = [r for r in results if r['common_count'] >= MIN_COMMON_URLS]
    not_in_cluster = [r for r in results if r['common_count'] < MIN_COMMON_URLS]
    
    print(f"\n✅ Запросов с >= {MIN_COMMON_URLS} общих URL: {len(in_cluster)}")
    print(f"❌ Запросов с < {MIN_COMMON_URLS} общих URL: {len(not_in_cluster)}")
    
    if in_cluster:
        print(f"\n📊 Топ-5 запросов с наибольшим пересечением:")
        sorted_results = sorted(results, key=lambda x: x['common_count'], reverse=True)
        for i, r in enumerate(sorted_results[:5], 1):
            print(f"  {i}. {r['query']}: {r['common_count']} общих URL")
            if r['common_urls']:
                print(f"     Примеры: {', '.join(list(r['common_urls'])[:3])}")
    
    # Детальный анализ для запросов с высоким пересечением
    print("\n" + "=" * 80)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ: Общие URL между 'скуд обои' и запросами с высоким пересечением")
    print("=" * 80)
    
    high_overlap = [r for r in results if r['common_count'] >= MIN_COMMON_URLS]
    if high_overlap:
        for r in sorted(high_overlap, key=lambda x: x['common_count'], reverse=True)[:5]:
            print(f"\n📌 {r['query']} ({r['common_count']} общих URL):")
            for url in list(r['common_urls'])[:10]:
                print(f"   • {url}")
    else:
        print("\n⚠️  Нет запросов с достаточным пересечением для попадания в кластер")
        print("   Это означает, что 'скуд обои' НЕ должен был попасть в этот кластер!")
        print("\n   Возможные причины:")
        print("   1. Данные в БД устарели")
        print("   2. Используется другой алгоритм кластеризации")
        print("   3. Параметры кластеризации изменились")


if __name__ == "__main__":
    main()

