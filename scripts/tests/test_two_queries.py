"""
Тест только двух запросов: "скуд в офис" и "система скуд в офис"
"""

import sys
sys.path.insert(0, '.')

from scripts.tests.test_clustering_no_transitive import (
    get_query_urls, calculate_url_overlap, can_add_to_cluster,
    cluster_queries_no_transitive, DB_PATH, GROUP_NAME, MIN_COMMON_URLS
)

queries = ["скуд в офис", "система скуд в офис"]

print("=" * 80)
print("ТЕСТ: Только два запроса")
print("=" * 80)

# Загружаем URL
query_urls_dict = {}
for query in queries:
    urls = get_query_urls(DB_PATH, GROUP_NAME, query)
    if urls:
        query_urls_dict[query] = urls
        print(f"\n✓ {query}: {len(urls)} URL")

if len(query_urls_dict) != 2:
    print("\n❌ Не все запросы найдены")
    exit(1)

# Проверяем связь
overlap = calculate_url_overlap(
    query_urls_dict["скуд в офис"],
    query_urls_dict["система скуд в офис"],
    top_n=20
)
print(f"\n📊 Связь между запросами: {overlap} общих URL")
print(f"   Порог для сильной связи: {MIN_COMMON_URLS * 2}")
print(f"   Является сильной связью: {overlap >= MIN_COMMON_URLS * 2}")

# Кластеризуем
print(f"\n{'=' * 80}")
print("КЛАСТЕРИЗАЦИЯ")
print("=" * 80)

clusters, debug_info = cluster_queries_no_transitive(
    list(query_urls_dict.keys()),
    query_urls_dict,
    MIN_COMMON_URLS,
    verbose=True
)

print(f"\n✓ Создано кластеров: {len(clusters)}")

for i, cluster in enumerate(clusters, 1):
    print(f"\n📦 Кластер {i} (размер: {len(cluster)}):")
    for query in cluster:
        print(f"   • {query}")

# Показываем debug info
print(f"\n{'=' * 80}")
print("DEBUG INFO")
print("=" * 80)

for query, info in debug_info.items():
    print(f"\n📌 {query}:")
    print(f"   Лучшая связь: '{info.get('best_match')}' ({info.get('best_match_overlap')} URL)")
    print(f"   Выбор: {info.get('chosen')}")
    if info.get('note'):
        print(f"   Примечание: {info['note']}")

