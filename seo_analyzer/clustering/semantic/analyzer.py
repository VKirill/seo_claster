"""Анализ семантических кластеров"""

from typing import Dict, List, Tuple
import numpy as np
import pandas as pd


def get_cluster_top_terms(
    cluster_id: int,
    cluster_labels: np.ndarray,
    tfidf_matrix,
    feature_names: np.ndarray,
    top_n: int = 10
) -> List[Tuple[str, float]]:
    """
    Возвращает топ-термины для кластера
    
    Args:
        cluster_id: ID кластера
        cluster_labels: Массив меток кластеров
        tfidf_matrix: TF-IDF матрица
        feature_names: Названия признаков
        top_n: Количество терминов
        
    Returns:
        Список (термин, вес)
    """
    if cluster_labels is None:
        return []
    
    # Находим документы в кластере
    cluster_mask = cluster_labels == cluster_id
    
    if not cluster_mask.any():
        return []
    
    # Суммируем TF-IDF веса по документам кластера
    cluster_tfidf = tfidf_matrix[cluster_mask].mean(axis=0).A1
    
    # Сортируем термины по весу
    top_indices = cluster_tfidf.argsort()[-top_n:][::-1]
    
    top_terms = [
        (feature_names[i], cluster_tfidf[i])
        for i in top_indices
    ]
    
    return top_terms


def get_cluster_summary(
    cluster_labels: np.ndarray,
    tfidf_matrix,
    feature_names: np.ndarray
) -> Dict[int, Dict]:
    """
    Возвращает сводку по всем кластерам
    
    Args:
        cluster_labels: Массив меток кластеров
        tfidf_matrix: TF-IDF матрица
        feature_names: Названия признаков
        
    Returns:
        Словарь с информацией о кластерах
    """
    if cluster_labels is None:
        return {}
    
    summary = {}
    unique_labels = set(cluster_labels)
    
    for label in unique_labels:
        if label == -1:  # Пропускаем шум в DBSCAN
            continue
        
        cluster_size = (cluster_labels == label).sum()
        top_terms = get_cluster_top_terms(label, cluster_labels, tfidf_matrix, feature_names, 10)
        
        # Генерируем название кластера из топ-3 терминов
        cluster_name = " / ".join([term for term, _ in top_terms[:3]])
        
        summary[int(label)] = {
            'cluster_id': int(label),
            'size': int(cluster_size),
            'top_terms': top_terms,
            'cluster_name': cluster_name,
        }
    
    return summary


def assign_cluster_names(
    df: pd.DataFrame,
    cluster_labels: np.ndarray = None,
    tfidf_matrix=None,
    feature_names: np.ndarray = None,
    use_exact_frequency: bool = True
) -> pd.DataFrame:
    """
    Присваивает названия кластерам в DataFrame
    
    Args:
        df: DataFrame с метками кластеров
        cluster_labels: Массив меток кластеров (опционально)
        tfidf_matrix: TF-IDF матрица (опционально)
        feature_names: Названия признаков (опционально)
        use_exact_frequency: Если True, использует запрос с макс частотностью
                            Если False, использует топ-термины TF-IDF
        
    Returns:
        DataFrame с названиями кластеров
    """
    if use_exact_frequency and 'frequency_exact' in df.columns:
        # Для каждого кластера берем запрос с максимальной частотностью
        cluster_names = {}
        
        for cluster_id in df['semantic_cluster_id'].unique():
            if cluster_id == -1:  # Пропускаем шум
                continue
            
            cluster_queries = df[df['semantic_cluster_id'] == cluster_id]
            
            # Выбираем запрос с максимальной frequency_exact
            best_query_idx = cluster_queries['frequency_exact'].idxmax()
            best_query = cluster_queries.loc[best_query_idx, 'keyword']
            
            cluster_names[cluster_id] = best_query
        
        df['cluster_name'] = df['semantic_cluster_id'].map(
            lambda x: cluster_names.get(x, f'Cluster {x}')
        )
    else:
        # Используем топ-термины TF-IDF (старый способ)
        if cluster_labels is not None and tfidf_matrix is not None and feature_names is not None:
            summary = get_cluster_summary(cluster_labels, tfidf_matrix, feature_names)
            
            df['cluster_name'] = df['semantic_cluster_id'].map(
                lambda x: summary.get(x, {}).get('cluster_name', f'Cluster {x}')
            )
        else:
            # Если нет данных TF-IDF, используем простые названия
            df['cluster_name'] = df['semantic_cluster_id'].map(
                lambda x: f'Cluster {x}' if x != -1 else 'Noise'
            )
    
    return df

