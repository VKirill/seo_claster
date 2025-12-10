"""Экспорт и статистика кластеров"""

from typing import Dict, Optional
import pandas as pd


def get_cluster_stats(clusters: List[Dict], query_to_cluster: Dict) -> Dict:
    """
    Возвращает статистику по кластерам
    
    Args:
        clusters: Список кластеров
        query_to_cluster: Маппинг запросов на кластеры
        
    Returns:
        Словарь со статистикой
    """
    if not clusters:
        return {}
    
    cluster_sizes = [len(c['queries']) for c in clusters]
    
    return {
        'total_clusters': len(clusters),
        'total_queries': sum(cluster_sizes),
        'avg_cluster_size': sum(cluster_sizes) / len(cluster_sizes),
        'min_cluster_size': min(cluster_sizes),
        'max_cluster_size': max(cluster_sizes),
        'orphan_queries': len([q for q in query_to_cluster if query_to_cluster[q] == -1])
    }


def add_to_dataframe(
    df: pd.DataFrame,
    clusters: List[Dict],
    query_to_cluster: Dict,
    query_column: str = 'keyword',
    cluster_column: str = 'word_match_cluster_id',
    cluster_name_column: str = 'word_match_cluster_name'
) -> pd.DataFrame:
    """
    Добавляет результаты кластеризации в DataFrame
    
    Args:
        df: DataFrame с запросами
        clusters: Список кластеров
        query_to_cluster: Маппинг запросов на кластеры
        query_column: Название колонки с запросами
        cluster_column: Название новой колонки для ID кластера
        cluster_name_column: Название новой колонки для названия кластера
        
    Returns:
        Обновленный DataFrame
    """
    print("🔄 Добавление кластеров в DataFrame...")
    
    # Создаем маппинг query -> cluster_id
    df[cluster_column] = df[query_column].map(
        lambda q: query_to_cluster.get(q, -1)
    )
    
    # Создаем названия кластеров (самая частотная фраза в кластере)
    cluster_names = {}
    for cluster in clusters:
        # Берем первую фразу (они отсортированы по частотности)
        cluster_names[cluster['cluster_id']] = cluster['queries'][0]
    
    df[cluster_name_column] = df[cluster_column].map(
        lambda cid: cluster_names.get(cid, 'Без кластера')
    )
    
    print(f"✓ Добавлены колонки: {cluster_column}, {cluster_name_column}")
    return df


def get_cluster_details(clusters: List[Dict], cluster_id: int) -> Optional[Dict]:
    """
    Возвращает детали конкретного кластера
    
    Args:
        clusters: Список кластеров
        cluster_id: ID кластера
        
    Returns:
        Словарь с информацией о кластере
    """
    for cluster in clusters:
        if cluster['cluster_id'] == cluster_id:
            return {
                'cluster_id': cluster_id,
                'cluster_name': cluster['queries'][0],
                'size': len(cluster['queries']),
                'queries': cluster['queries'],
                'common_words': list(cluster['tokens'])
            }
    return None


def export_clusters(clusters: List[Dict]) -> pd.DataFrame:
    """
    Экспортирует кластеры в DataFrame
    
    Args:
        clusters: Список кластеров
        
    Returns:
        DataFrame со всеми кластерами
    """
    rows = []
    
    for cluster in clusters:
        for query in cluster['queries']:
            rows.append({
                'cluster_id': cluster['cluster_id'],
                'cluster_name': cluster['queries'][0],
                'cluster_size': len(cluster['queries']),
                'query': query,
                'common_words': ', '.join(sorted(cluster['tokens']))
            })
    
    return pd.DataFrame(rows)

