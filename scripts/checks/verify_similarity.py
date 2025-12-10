"""Проверка - почему разная схожесть"""
import pandas as pd
from seo_analyzer.clustering.serp_advanced_clusterer import AdvancedSERPClusterer
from seo_analyzer.core.serp_database import SERPDatabase

queries = ["карта скуд", "комплект скуд"]

db = SERPDatabase()
data = []
for query in queries:
    serp_data = db.get_serp_data(query)
    if serp_data and 'documents' in serp_data:
        urls = [doc.get('url', '') for doc in serp_data['documents'] if doc.get('url')]
    else:
        urls = []
    data.append({'keyword': query, 'serp_urls': urls})
    print(f"\n{query}:")
    print(f"  Всего URL: {len(urls)}")
    print(f"  Первые 5: {urls[:5]}")

clusterer = AdvancedSERPClusterer(
    min_common_urls=7,
    top_positions=30,
    max_cluster_size=50,
    mode="strict",
    position_weights=True,
    semantic_check=True,
    min_cluster_cohesion=0.7
)

urls1 = data[0]['serp_urls']
urls2 = data[1]['serp_urls']

print("\n" + "="*80)
print("ПРОВЕРКА СХОЖЕСТИ")
print("="*80)

# Вариант 1: как в тесте
common1, score1 = clusterer.calculate_weighted_similarity(urls1, urls2)
print(f"\n'{queries[0]}' → '{queries[1]}':")
print(f"  Общих URL: {common1}, Score: {score1:.1f}")

# Вариант 2: обратный порядок
common2, score2 = clusterer.calculate_weighted_similarity(urls2, urls1)
print(f"\n'{queries[1]}' → '{queries[0]}':")
print(f"  Общих URL: {common2}, Score: {score2:.1f}")

# Вариант 3: проверяем пересечение вручную
set1 = set(urls1[:30])
set2 = set(urls2[:30])
common_set = set1 & set2

print(f"\nРучная проверка (set intersection):")
print(f"  len(urls1[:30]): {len(set1)}")
print(f"  len(urls2[:30]): {len(set2)}")
print(f"  Пересечение: {len(common_set)}")
print(f"  Общие URL: {list(common_set)[:5]}")

# Извлекаем домены
from seo_analyzer.clustering.serp_advanced_clusterer import AdvancedSERPClusterer
test_clusterer = AdvancedSERPClusterer()

domains1 = []
for url in urls1[:30]:
    domain = test_clusterer._extract_domain(url)
    if domain:
        domains1.append(domain)

domains2 = []
for url in urls2[:30]:
    domain = test_clusterer._extract_domain(url)
    if domain:
        domains2.append(domain)

common_domains = set(domains1) & set(domains2)

print(f"\nПосле извлечения доменов:")
print(f"  Домены 1: {len(set(domains1))}")
print(f"  Домены 2: {len(set(domains2))}")
print(f"  Общие домены: {len(common_domains)}")
print(f"  Список: {list(common_domains)[:10]}")

