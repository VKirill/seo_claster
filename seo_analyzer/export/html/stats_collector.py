"""Сбор статистики и данных для дашборда"""

from typing import Dict, List
import pandas as pd


def collect_stats(df: pd.DataFrame) -> Dict:
    """
    Собирает общую статистику из DataFrame
    
    Args:
        df: DataFrame с результатами
        
    Returns:
        Словарь со статистикой
    """
    stats = {
        'total_queries': len(df),
        'total_frequency': int(df['frequency_world'].sum()) if 'frequency_world' in df.columns else 0,
        'avg_frequency': round(df['frequency_world'].mean(), 1) if 'frequency_world' in df.columns else 0,
    }
    
    # Кластеры
    if 'semantic_cluster_id' in df.columns:
        stats['n_clusters'] = int(df['semantic_cluster_id'].nunique())
    
    # Интенты
    if 'main_intent' in df.columns:
        stats['intent_dist'] = df['main_intent'].value_counts().to_dict()
    
    # Воронка
    if 'funnel_stage' in df.columns:
        stats['funnel_dist'] = df['funnel_stage'].value_counts().to_dict()
    
    # Бренды
    if 'is_brand_query' in df.columns:
        stats['branded_count'] = int(df['is_brand_query'].sum())
        stats['brand_percent'] = round((df['is_brand_query'].sum() / len(df)) * 100, 1)
    
    # География
    if 'has_geo' in df.columns:
        stats['geo_count'] = int(df['has_geo'].sum())
        stats['geo_percent'] = round((df['has_geo'].sum() / len(df)) * 100, 1)
    
    return stats


def collect_clusters_data(df: pd.DataFrame) -> List[Dict]:
    """
    Собирает детальные данные по кластерам
    
    Args:
        df: DataFrame с результатами
        
    Returns:
        Список словарей с данными кластеров
    """
    if 'semantic_cluster_id' not in df.columns:
        return []
    
    clusters = []
    
    for cluster_id in df['semantic_cluster_id'].unique():
        if pd.isna(cluster_id):
            continue
        
        cluster_df = df[df['semantic_cluster_id'] == cluster_id]
        
        cluster_info = {
            'id': int(cluster_id),
            'name': cluster_df['cluster_name'].iloc[0] if 'cluster_name' in cluster_df.columns else f'Кластер {int(cluster_id)}',
            'size': len(cluster_df),
            'total_freq': int(cluster_df['frequency_world'].sum()) if 'frequency_world' in cluster_df.columns else 0,
            'avg_freq': round(cluster_df['frequency_world'].mean(), 1) if 'frequency_world' in cluster_df.columns else 0,
        }
        
        # Основной интент
        if 'main_intent' in cluster_df.columns:
            cluster_info['main_intent'] = cluster_df['main_intent'].mode().iloc[0] if len(cluster_df['main_intent'].mode()) > 0 else 'unknown'
        
        # Воронка
        if 'funnel_stage' in cluster_df.columns:
            cluster_info['funnel_stage'] = cluster_df['funnel_stage'].mode().iloc[0] if len(cluster_df['funnel_stage'].mode()) > 0 else 'unknown'
        
        # Целевая страница
        if 'suggested_url' in cluster_df.columns:
            cluster_info['suggested_url'] = cluster_df['suggested_url'].iloc[0] if not pd.isna(cluster_df['suggested_url'].iloc[0]) else ''
        
        # Топ-30 запросов
        top_queries = cluster_df.nlargest(30, 'frequency_world')[['keyword', 'frequency_world']].to_dict('records') if 'frequency_world' in cluster_df.columns else []
        cluster_info['top_queries'] = top_queries
        
        clusters.append(cluster_info)
    
    # Сортируем по частотности
    clusters.sort(key=lambda x: x['total_freq'], reverse=True)
    
    return clusters

