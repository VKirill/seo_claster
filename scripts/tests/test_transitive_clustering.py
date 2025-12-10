"""
Проверка транзитивного замыкания кластеризации
Проверяет, попадает ли "скуд обои" в кластер через транзитивные связи
"""

import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Set

DB_PATH = Path("output/master_queries.db")
GROUP_NAME = "скуд"
MIN_COMMON_URLS = 7

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
    "скуд обои",
    "скуд рим",
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


def build_similarity_graph(query_urls_dict: Dict[str, List[str]], min_common: int) -> Dict[str, Set[str]]:
    """Строит граф связей между запросами"""
    graph = {query: set() for query in query_urls_dict.keys()}
    
    queries = list(query_urls_dict.keys())
    for i, query1 in enumerate(queries):
        for query2 in queries[i+1:]:
            overlap = calculate_url_overlap(
                query_urls_dict[query1],
                query_urls_dict[query2]
            )
            if overlap >= min_common:
                graph[query1].add(query2)
                graph[query2].add(query1)
    
    return graph


def find_connected_component(graph: Dict[str, Set[str]], start_query: str) -> Set[str]:
    """Находит все запросы, связанные с start_query через транзитивное замыкание"""
    visited = set()
    stack = [start_query]
    component = set()
    
    while stack:
        query = stack.pop()
        if query in visited:
            continue
        visited.add(query)
        component.add(query)
        
        for neighbor in graph.get(query, set()):
            if neighbor not in visited:
                stack.append(neighbor)
    
    return component


def main():
    print("=" * 80)
    print("ПРОВЕРКА ТРАНЗИТИВНОГО ЗАМЫКАНИЯ КЛАСТЕРИЗАЦИИ")
    print("=" * 80)
    print(f"\nПорог: {MIN_COMMON_URLS} общих URL")
    print()
    
    # Загружаем URL
    print("📥 Загрузка URL...")
    query_urls_dict = {}
    for query in QUERIES:
        urls = get_query_urls(DB_PATH, GROUP_NAME, query)
        if urls:
            query_urls_dict[query] = urls
    
    print(f"✓ Загружено: {len(query_urls_dict)} запросов\n")
    
    # Строим граф связей
    print("🔗 Построение графа связей...")
    graph = build_similarity_graph(query_urls_dict, MIN_COMMON_URLS)
    
    # Подсчитываем связи
    total_edges = sum(len(neighbors) for neighbors in graph.values()) // 2
    print(f"✓ Создано связей: {total_edges}")
    
    # Находим компоненту связности для "скуд обои"
    target_query = "скуд обои"
    if target_query not in query_urls_dict:
        print(f"\n❌ Запрос '{target_query}' не найден!")
        return
    
    component = find_connected_component(graph, target_query)
    
    print(f"\n{'=' * 80}")
    print(f"РЕЗУЛЬТАТ: Компонента связности для '{target_query}'")
    print("=" * 80)
    print(f"\n✅ Запросов в кластере: {len(component)}")
    print(f"\nСписок запросов в кластере:")
    for i, query in enumerate(sorted(component), 1):
        print(f"  {i}. {query}")
    
    # Проверяем, все ли запросы из списка в кластере
    all_in_cluster = set(QUERIES).issubset(component)
    print(f"\n{'=' * 80}")
    print("АНАЛИЗ")
    print("=" * 80)
    
    if all_in_cluster:
        print(f"\n✅ ВСЕ запросы из списка попадают в один кластер!")
        print(f"   Это объясняет, почему '{target_query}' попал в кластер со всеми запросами.")
    else:
        missing = set(QUERIES) - component
        print(f"\n⚠️  НЕ все запросы в кластере:")
        print(f"   В кластере: {len(component)} из {len(QUERIES)}")
        print(f"   Отсутствуют: {len(missing)}")
        for q in missing:
            print(f"     - {q}")
    
    # Показываем прямые связи "скуд обои"
    print(f"\n{'=' * 80}")
    print(f"ПРЯМЫЕ СВЯЗИ '{target_query}' (>= {MIN_COMMON_URLS} общих URL)")
    print("=" * 80)
    direct_links = graph.get(target_query, set())
    if direct_links:
        print(f"\n✅ Прямых связей: {len(direct_links)}")
        for link in sorted(direct_links):
            overlap = calculate_url_overlap(
                query_urls_dict[target_query],
                query_urls_dict[link]
            )
            print(f"   • {link} ({overlap} общих URL)")
    else:
        print(f"\n❌ Нет прямых связей с порогом {MIN_COMMON_URLS}")


if __name__ == "__main__":
    main()

