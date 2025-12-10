"""Отладка - смотрим реальную схожесть внутри кластера"""
import pandas as pd
from seo_analyzer.clustering.serp_advanced_clusterer import AdvancedSERPClusterer
from seo_analyzer.core.serp_database import SERPDatabase

# Запросы из проблемного кластера
CLUSTER_QUERIES = [
    "карта скуд",
    "комплект скуд",
    "скуд купить",
    "скуд на дверь",
    "скуд болид",
    "сигур скуд",
    "скуд рубеж",
    "электромагнитный замок скуд",
    "скуд цена",
    "скуд на болиде",
    "скуд на одну дверь",
    "rfid карта скуд",
]

def load_serp(queries):
    db = SERPDatabase()
    data = []
    for query in queries:
        serp_data = db.get_serp_data(query)
        if serp_data and 'documents' in serp_data:
            urls = [doc.get('url', '') for doc in serp_data['documents'] if doc.get('url')]
        else:
            urls = []
        data.append({'keyword': query, 'serp_urls': urls})
    return pd.DataFrame(data)

df = load_serp(CLUSTER_QUERIES)

clusterer = AdvancedSERPClusterer(
    min_common_urls=7,
    top_positions=30,
    max_cluster_size=50,
    mode="strict",
    position_weights=True,
    semantic_check=True,
    min_cluster_cohesion=0.7
)

# Создаем словарь URL
query_urls = {}
for _, row in df.iterrows():
    query_urls[row['keyword']] = row['serp_urls']

print("="*80)
print("🔍 МАТРИЦА СХОЖЕСТИ ЗАПРОСОВ В ПРОБЛЕМНОМ КЛАСТЕРЕ")
print("="*80)
print("\nЛегенда: ✅ ≥7 общих URL | ⚠️ 4-6 общих | ❌ <4 общих\n")

# Матрица схожести
queries = CLUSTER_QUERIES
print(f"{'Запрос':<40} ", end='')
for i, q in enumerate(queries[:5]):  # Показываем первые 5 для компактности
    print(f"{i+1:>3}", end=' ')
print("...")

for i, q1 in enumerate(queries):
    print(f"{i+1:>2}. {q1:<37} ", end='')
    
    for j, q2 in enumerate(queries[:5]):
        if i == j:
            print(" - ", end=' ')
            continue
        
        urls1 = query_urls.get(q1, [])
        urls2 = query_urls.get(q2, [])
        
        if urls1 and urls2:
            common, score = clusterer.calculate_weighted_similarity(urls1, urls2)
            if common >= 7:
                print(f"✅{common:>2}", end=' ')
            elif common >= 4:
                print(f"⚠️{common:>2}", end=' ')
            else:
                print(f"❌{common:>2}", end=' ')
        else:
            print(" ? ", end=' ')
    print("...")

print("\n" + "="*80)
print("❓ ВОПРОС: Почему они в одном кластере?")
print("="*80)

# Проверяем цепочки связей
print("\n🔗 Цепочки связей (кто с кем напрямую связан):")
print("-"*80)

for q1 in queries:
    connected_to = []
    urls1 = query_urls.get(q1, [])
    
    for q2 in queries:
        if q1 == q2:
            continue
        urls2 = query_urls.get(q2, [])
        if urls1 and urls2:
            common, score = clusterer.calculate_weighted_similarity(urls1, urls2)
            if common >= 7:
                connected_to.append(f"{q2} ({common})")
    
    if connected_to:
        print(f"\n'{q1}'")
        print(f"  ↔ {', '.join(connected_to)}")

print("\n" + "="*80)
print("💡 ВЫВОД:")
print("="*80)
print("""
Если все запросы связаны через ЦЕПОЧКУ (A→B→C), то даже в STRICT режиме
они попадут в один кластер, потому что алгоритм НЕ проверяет связь КАЖДОГО
запроса с КАЖДЫМ. Он только проверяет связь нового запроса с существующими.

РЕШЕНИЕ: Нужен режим ISOLATED где каждый запрос проверяется со ВСЕМИ
в кластере, а не через транзитивность.
""")

