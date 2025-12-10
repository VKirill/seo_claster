"""Применение связей между кластерами к DataFrame"""

from typing import Dict, List, Tuple
import pandas as pd


def apply_cluster_relationships(
    df: pd.DataFrame,
    relationships: Dict[int, List[Tuple[int, str, int]]],
    cluster_column: str = 'semantic_cluster_id'
) -> pd.DataFrame:
    """
    Добавляет колонку с связанными кластерами в DataFrame.
    
    Args:
        df: DataFrame с кластерами
        relationships: Словарь связей {cluster_id: [(related_id, name, strength)]}
        cluster_column: Название колонки с ID кластера
        
    Returns:
        DataFrame с новой колонкой 'related_clusters'
    """
    print("📝 Добавление связанных кластеров в DataFrame...")
    
    if cluster_column not in df.columns:
        print("  ⚠️  Колонка кластера не найдена")
        return df
    
    # Создаем маппинг cluster_id -> список названий связанных кластеров
    cluster_to_related = {}
    
    for cluster_id, relations in relationships.items():
        # Берём только названия кластеров (без силы связи)
        related_names = [name for _, name, _ in relations]
        cluster_to_related[cluster_id] = ', '.join(related_names)
    
    # Применяем к DataFrame
    df['related_clusters'] = df[cluster_column].map(
        lambda cid: cluster_to_related.get(cid, '')
    )
    
    # Считаем статистику
    non_empty = df['related_clusters'].astype(bool).sum()
    print(f"✓ Добавлена колонка 'related_clusters' ({non_empty}/{len(df)} запросов имеют связи)")
    
    return df


def get_related_clusters_list(
    cluster_id: int,
    relationships: Dict[int, List[Tuple[int, str, int]]]
) -> List[str]:
    """
    Получает список названий связанных кластеров для JSON экспорта.
    
    Args:
        cluster_id: ID кластера
        relationships: Словарь связей
        
    Returns:
        Список названий связанных кластеров
    """
    if cluster_id not in relationships:
        return []
    
    return [name for _, name, _ in relationships[cluster_id]]


def get_related_clusters_detailed(
    cluster_id: int,
    relationships: Dict[int, List[Tuple[int, str, int]]]
) -> List[Dict]:
    """
    Получает детальную информацию о связанных кластерах для JSON.
    
    Args:
        cluster_id: ID кластера
        relationships: Словарь связей
        
    Returns:
        Список словарей с информацией о связях
    """
    if cluster_id not in relationships:
        return []
    
    return [
        {
            'cluster_id': rel_id,
            'cluster_name': name,
            'link_strength': strength
        }
        for rel_id, name, strength in relationships[cluster_id]
    ]

