"""Корректировка интента на основе SERP данных"""

import pandas as pd
from typing import Dict


def adjust_intent_by_serp(
    df: pd.DataFrame,
    intent_column: str = 'main_intent',
    commercial_domains_column: str = 'serp_commercial_domains',
    info_domains_column: str = 'serp_info_domains',
    commercial_threshold: float = 0.6
) -> pd.DataFrame:
    """
    Корректирует интент на основе анализа SERP данных
    
    Логика:
    - Если в выдаче преобладают коммерческие домены (>60%), 
      а интент = informational → меняем на commercial
    - Если в выдаче преобладают инфо домены (>60%),
      а интент = commercial → меняем на informational
    
    Args:
        df: DataFrame с данными
        intent_column: Название колонки с интентом
        commercial_domains_column: Колонка с количеством коммерческих доменов
        info_domains_column: Колонка с количеством информационных доменов
        commercial_threshold: Порог для определения коммерческого интента (по умолчанию 0.6 = 60%)
        
    Returns:
        DataFrame с скорректированными интентами
    """
    if intent_column not in df.columns:
        return df
    
    if commercial_domains_column not in df.columns or info_domains_column not in df.columns:
        print("⚠️  SERP данные не найдены, корректировка интента пропущена")
        return df
    
    # Создаем копию для безопасности
    df = df.copy()
    
    # Счётчики изменений
    changed_to_commercial = 0
    changed_to_informational = 0
    
    # Итерируем по строкам где есть SERP данные
    for idx in df.index:
        commercial_count = df.at[idx, commercial_domains_column]
        info_count = df.at[idx, info_domains_column]
        current_intent = df.at[idx, intent_column]
        
        # Пропускаем строки без SERP данных
        total_domains = commercial_count + info_count
        if total_domains == 0:
            continue
        
        # Вычисляем соотношение
        commercial_ratio = commercial_count / total_domains
        info_ratio = info_count / total_domains
        
        # Корректировка informational → commercial
        if current_intent == 'informational' and commercial_ratio >= commercial_threshold:
            df.at[idx, intent_column] = 'commercial'
            df.at[idx, 'commercial_score'] = commercial_ratio * 10.0
            changed_to_commercial += 1
        
        # Корректировка commercial → informational
        elif current_intent == 'commercial' and info_ratio >= commercial_threshold:
            df.at[idx, intent_column] = 'informational'
            df.at[idx, 'informational_score'] = info_ratio * 10.0
            changed_to_informational += 1
        
        # Корректировка commercial_geo → informational_geo (если в SERP преобладают инфо домены)
        elif current_intent == 'commercial_geo' and info_ratio >= commercial_threshold:
            df.at[idx, intent_column] = 'informational_geo'
            df.at[idx, 'informational_score'] = info_ratio * 10.0
            changed_to_informational += 1
        
        # Корректировка commercial_geo → commercial (если есть SERP данные и коммерческие домены)
        elif current_intent == 'commercial_geo' and commercial_ratio >= commercial_threshold:
            # Оставляем commercial_geo, но обновляем скор
            df.at[idx, 'commercial_score'] = commercial_ratio * 10.0
        
        # Корректировка informational_geo → commercial_geo (если в SERP преобладают коммерческие домены)
        elif current_intent == 'informational_geo' and commercial_ratio >= commercial_threshold:
            df.at[idx, intent_column] = 'commercial_geo'
            df.at[idx, 'commercial_score'] = commercial_ratio * 10.0
            changed_to_commercial += 1
    
    # Логирование изменений
    if changed_to_commercial > 0 or changed_to_informational > 0:
        print(f"✓ Корректировка интента по SERP:")
        print(f"  → commercial/commercial_geo: {changed_to_commercial}")
        print(f"  → informational/informational_geo: {changed_to_informational}")
    
    return df


def get_serp_intent_statistics(df: pd.DataFrame) -> Dict[str, any]:
    """
    Получить статистику по корректировке интента
    
    Args:
        df: DataFrame с данными
        
    Returns:
        Словарь со статистикой
    """
    if 'main_intent' not in df.columns:
        return {}
    
    intent_counts = df['main_intent'].value_counts().to_dict()
    
    return {
        'total_queries': len(df),
        'intent_distribution': intent_counts,
        'commercial_percent': intent_counts.get('commercial', 0) / len(df) * 100 if len(df) > 0 else 0,
        'informational_percent': intent_counts.get('informational', 0) / len(df) * 100 if len(df) > 0 else 0,
    }


