"""Построение кластеров: первичная группировка"""

from typing import Dict, List, Optional
from .matcher import tokenize_query, find_best_cluster
from .scorer import count_matches


def build_initial_clusters(
    queries: List[str],
    frequencies: Optional[Dict[str, int]],
    min_match_strength: int,
    exclude_stopwords: bool
) -> List[Dict]:
    """
    Создает первичные кластеры из запросов
    
    Args:
        queries: Список запросов
        frequencies: Словарь частот для сортировки
        min_match_strength: Минимальная сила совпадения
        exclude_stopwords: Исключать стоп-слова
        
    Returns:
        Список первичных кластеров
    """
    # Сортируем запросы по частотности (если есть)
    if frequencies:
        sorted_queries = sorted(
            queries,
            key=lambda q: frequencies.get(q, 0),
            reverse=True
        )
    else:
        sorted_queries = queries.copy()
    
    clusters = []
    processed = set()
    
    # Первичная группировка
    for query in sorted_queries:
        if query in processed:
            continue
        
        query_tokens = tokenize_query(query, exclude_stopwords)
        
        # Ищем существующий кластер
        best_cluster_idx = find_best_cluster(
            query_tokens,
            clusters,
            min_match_strength,
            count_matches
        )
        
        if best_cluster_idx is not None:
            # Добавляем в существующий кластер
            clusters[best_cluster_idx]['queries'].append(query)
            clusters[best_cluster_idx]['tokens'].update(query_tokens)
        else:
            # Создаем новый кластер
            clusters.append({
                'queries': [query],
                'tokens': query_tokens.copy(),
                'cluster_id': len(clusters)
            })
        
        processed.add(query)
    
    return clusters


def filter_and_number_clusters(
    clusters: List[Dict],
    min_group_size: int
) -> tuple[List[Dict], List[str]]:
    """
    Фильтрует кластеры по минимальному размеру и нумерует их
    
    Args:
        clusters: Список кластеров
        min_group_size: Минимальный размер группы
        
    Returns:
        Кортеж (валидные кластеры, одиночные запросы)
    """
    valid_clusters = []
    orphan_queries = []
    
    for cluster in clusters:
        if len(cluster['queries']) >= min_group_size:
            valid_clusters.append(cluster)
        else:
            orphan_queries.extend(cluster['queries'])
    
    # Нумеруем финальные кластеры
    for idx, cluster in enumerate(valid_clusters):
        cluster['cluster_id'] = idx
    
    return valid_clusters, orphan_queries

