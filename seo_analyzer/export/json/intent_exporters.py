
"""Экспорт кластеров по интентам"""

import json
from pathlib import Path
from typing import Dict, List, Callable
import pandas as pd


def export_commercial_clusters(
    df: pd.DataFrame,
    output_path: Path,
    build_cluster_info_fn: Callable,
    cluster_column: str = 'semantic_cluster_id',
    indent: int = 2
) -> bool:
    """
    Экспортирует только полностью коммерческие кластеры
    
    Кластер считается коммерческим если ВСЕ запросы имеют main_intent = 'commercial'
    
    Args:
        df: DataFrame с результатами
        output_path: Путь для сохранения
        build_cluster_info_fn: Функция построения информации о кластере
        cluster_column: Колонка кластера
        indent: Отступ для JSON
        
    Returns:
        True если успешно
    """
    try:
        print(f"💾 Экспорт коммерческих кластеров: {output_path.name}...")
        
        # Проверяем наличие нужных колонок
        if cluster_column not in df.columns or 'main_intent' not in df.columns:
            print(f"⚠️  Пропускаем экспорт: нет колонок {cluster_column} или main_intent")
            return False
        
        # Фильтруем полностью коммерческие кластеры
        commercial_cluster_ids = _filter_pure_intent_clusters(df, cluster_column, 'commercial')
        
        if not commercial_cluster_ids:
            print("  ℹ️  Полностью коммерческих кластеров не найдено")
            return False
        
        # Фильтруем DataFrame
        commercial_df = df[df[cluster_column].isin(commercial_cluster_ids)]
        
        # Строим иерархическую структуру
        hierarchy = _build_hierarchy_structure(
            commercial_df,
            commercial_cluster_ids,
            'СКУД - Коммерческие запросы',
            'commercial'
        )
        
        # Группируем по кластерам
        for cluster_id in commercial_cluster_ids:
            cluster_df = commercial_df[commercial_df[cluster_column] == cluster_id]
            cluster_info = build_cluster_info_fn(cluster_id, cluster_df)
            hierarchy['subclusters'].append(cluster_info)
        
        # Сохраняем
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(hierarchy, f, ensure_ascii=False, indent=indent)
        
        print(f"✓ Экспортировано {len(commercial_cluster_ids)} коммерческих кластеров ({len(commercial_df)} запросов)")
        return True
        
    except Exception as e:
        print(f"⚠️ Ошибка экспорта коммерческих кластеров: {e}")
        return False


def export_informational_clusters(
    df: pd.DataFrame,
    output_path: Path,
    build_cluster_info_fn: Callable,
    cluster_column: str = 'semantic_cluster_id',
    indent: int = 2
) -> bool:
    """
    Экспортирует только полностью информационные кластеры
    
    Кластер считается информационным если ВСЕ запросы имеют main_intent = 'informational'
    
    Args:
        df: DataFrame с результатами
        output_path: Путь для сохранения
        build_cluster_info_fn: Функция построения информации о кластере
        cluster_column: Колонка кластера
        indent: Отступ для JSON
        
    Returns:
        True если успешно
    """
    try:
        print(f"💾 Экспорт информационных кластеров: {output_path.name}...")
        
        # Проверяем наличие нужных колонок
        if cluster_column not in df.columns or 'main_intent' not in df.columns:
            print(f"⚠️  Пропускаем экспорт: нет колонок {cluster_column} или main_intent")
            return False
        
        # Фильтруем полностью информационные кластеры
        informational_cluster_ids = _filter_pure_intent_clusters(df, cluster_column, 'informational')
        
        if not informational_cluster_ids:
            print("  ℹ️  Полностью информационных кластеров не найдено")
            return False
        
        # Фильтруем DataFrame
        informational_df = df[df[cluster_column].isin(informational_cluster_ids)]
        
        # Строим иерархическую структуру
        hierarchy = _build_hierarchy_structure(
            informational_df,
            informational_cluster_ids,
            'СКУД - Информационные запросы',
            'informational'
        )
        
        # Группируем по кластерам
        for cluster_id in informational_cluster_ids:
            cluster_df = informational_df[informational_df[cluster_column] == cluster_id]
            cluster_info = build_cluster_info_fn(cluster_id, cluster_df)
            hierarchy['subclusters'].append(cluster_info)
        
        # Сохраняем
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(hierarchy, f, ensure_ascii=False, indent=indent)
        
        print(f"✓ Экспортировано {len(informational_cluster_ids)} информационных кластеров ({len(informational_df)} запросов)")
        return True
        
    except Exception as e:
        print(f"⚠️ Ошибка экспорта информационных кластеров: {e}")
        return False


def _filter_pure_intent_clusters(
    df: pd.DataFrame,
    cluster_column: str,
    intent: str
) -> List:
    """
    Фильтрует кластеры с чистым интентом
    
    Args:
        df: DataFrame
        cluster_column: Колонка кластера
        intent: Интент ('commercial' или 'informational')
        
    Returns:
        Список ID кластеров
    """
    cluster_ids = []
    for cluster_id, cluster_df in df.groupby(cluster_column):
        # Все запросы кластера должны иметь одинаковый интент
        intents = cluster_df['main_intent'].unique()
        if len(intents) == 1 and intents[0] == intent:
            cluster_ids.append(cluster_id)
    return cluster_ids


def _build_hierarchy_structure(
    df: pd.DataFrame,
    cluster_ids: List,
    main_cluster_name: str,
    intent_filter: str
) -> Dict:
    """Строит базовую иерархическую структуру"""
    return {
        'main_cluster': main_cluster_name,
        'intent_filter': intent_filter,
        'total_queries': len(df),
        'total_clusters': len(cluster_ids),
        'total_frequency': int(pd.to_numeric(df['frequency_world'], errors='coerce').sum()) if 'frequency_world' in df.columns else 0,
        'subclusters': []
    }

