"""Группировка данных кластеров для анализа связей"""

from typing import Dict
import pandas as pd


def group_clusters_data(
    df: pd.DataFrame,
    cluster_column: str = 'semantic_cluster_id'
) -> Dict[int, Dict]:
    """
    Группирует данные по кластерам для анализа связей.
    
    Args:
        df: DataFrame с кластерами
        cluster_column: Название колонки с ID кластера
        
    Returns:
        Dict[cluster_id, cluster_data] где cluster_data содержит:
        - name: название кластера
        - urls: множество URL из SERP
        - words: множество ключевых слов
        - intent: основной интент
        - funnel: этап воронки
        - size: размер кластера
    """
    cluster_data = {}
    
    for cluster_id, group in df.groupby(cluster_column):
        if cluster_id == -1:  # Пропускаем неклассифицированные
            continue
        
        # Собираем URLs из SERP всех запросов кластера
        all_urls = set()
        if 'serp_urls' in group.columns:
            for serp_urls in group['serp_urls'].dropna():
                if isinstance(serp_urls, (list, tuple)):
                    all_urls.update(serp_urls[:20])  # Топ-20
                elif isinstance(serp_urls, str):
                    all_urls.update(serp_urls.split(',')[:20])
        
        # Собираем ключевые слова из всех запросов
        all_words = set()
        for query in group['keyword']:
            all_words.update(str(query).lower().split())
        
        # Получаем название кластера
        cluster_name = group['cluster_name'].iloc[0] if 'cluster_name' in group.columns else f"Кластер {cluster_id}"
        
        # Получаем интент и воронку (для улучшения связей)
        intent = group['main_intent'].iloc[0] if 'main_intent' in group.columns else None
        funnel = group['funnel_stage'].iloc[0] if 'funnel_stage' in group.columns else None
        
        cluster_data[cluster_id] = {
            'name': cluster_name,
            'urls': all_urls,
            'words': all_words,
            'intent': intent,
            'funnel': funnel,
            'size': len(group)
        }
    
    return cluster_data


def filter_stopwords(words: set) -> set:
    """
    Фильтрует стоп-слова из множества.
    
    Args:
        words: Множество слов
        
    Returns:
        Множество слов без стоп-слов
    """
    stopwords = {
        'и', 'в', 'на', 'с', 'для', 'по', 'от', 'до', 'из', 'к',
        'о', 'у', 'за', 'при', 'про', 'без', 'через', 'под', 'над'
    }
    return words - stopwords

