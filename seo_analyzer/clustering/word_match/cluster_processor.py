"""Обработка кластеров: усиление связей и перераспределение"""

from typing import Dict, List
from .matcher import tokenize_query, find_best_cluster
from .scorer import count_matches


def strengthen_cluster_links(
    clusters: List[Dict],
    exclude_stopwords: bool
) -> List[Dict]:
    """
    Усиливает связи между фразами в кластерах
    
    Проверяет, есть ли у фраз более тесные связи с другими кластерами
    и перемещает их при необходимости.
    
    Args:
        clusters: Список кластеров
        exclude_stopwords: Исключать стоп-слова из подсчета совпадений
        
    Returns:
        Обновленный список кластеров
    """
    moves_count = 0
    
    for cluster_idx, cluster in enumerate(clusters):
        queries_to_move = []
        
        for query in cluster['queries']:
            query_tokens = tokenize_query(query, exclude_stopwords)
            
            # Текущее количество совпадений в своем кластере
            current_avg_matches = 0
            for other_query in cluster['queries']:
                if other_query != query:
                    other_tokens = tokenize_query(other_query, exclude_stopwords)
                    current_avg_matches += count_matches(query_tokens, other_tokens)
            
            current_avg_matches /= max(len(cluster['queries']) - 1, 1)
            
            # Ищем кластер с более тесными связями
            best_other_cluster = None
            best_matches = current_avg_matches
            
            for other_idx, other_cluster in enumerate(clusters):
                if other_idx == cluster_idx:
                    continue
                
                # Пропускаем пустые кластеры
                if not other_cluster['queries']:
                    continue
                
                total_matches = 0
                for other_query in other_cluster['queries']:
                    other_tokens = tokenize_query(other_query, exclude_stopwords)
                    total_matches += count_matches(query_tokens, other_tokens)
                
                avg_matches = total_matches / len(other_cluster['queries'])
                
                # Если связь сильнее, запоминаем
                if avg_matches > best_matches:
                    best_matches = avg_matches
                    best_other_cluster = other_idx
            
            # Если найден лучший кластер, помечаем для перемещения
            if best_other_cluster is not None:
                queries_to_move.append((query, best_other_cluster))
        
        # Перемещаем фразы
        for query, target_cluster_idx in queries_to_move:
            # Проверяем что не последняя фраза в кластере
            if len(cluster['queries']) > 1:
                cluster['queries'].remove(query)
                clusters[target_cluster_idx]['queries'].append(query)
                moves_count += 1
    
    if moves_count > 0:
        print(f"      Перемещено фраз: {moves_count}")
    
    # Удаляем пустые кластеры после перемещения
    clusters = [c for c in clusters if c['queries']]
    
    return clusters


def redistribute_orphans(
    orphan_queries: List[str],
    clusters: List[Dict],
    min_match_strength: int,
    exclude_stopwords: bool
) -> int:
    """
    Перераспределяет одиночные фразы по существующим кластерам
    
    Args:
        orphan_queries: Список одиночных фраз
        clusters: Список существующих кластеров
        min_match_strength: Минимальная сила совпадения
        exclude_stopwords: Исключать стоп-слова из подсчета совпадений
        
    Returns:
        Количество перераспределенных фраз
    """
    redistributed = 0
    
    for query in orphan_queries:
        query_tokens = tokenize_query(query, exclude_stopwords)
        
        # Ищем наилучший кластер с пониженным порогом
        best_cluster_idx = find_best_cluster(
            query_tokens,
            clusters,
            max(1, min_match_strength - 1),  # Снижаем порог на 1
            count_matches
        )
        
        if best_cluster_idx is not None:
            clusters[best_cluster_idx]['queries'].append(query)
            redistributed += 1
    
    return redistributed

