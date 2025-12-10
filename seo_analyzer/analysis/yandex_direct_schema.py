"""
Схема данных Yandex Direct для DataFrame.

Определяет колонки которые добавляются при интеграции с Direct API.
"""

import pandas as pd


# Схема колонок Direct
DIRECT_COLUMNS = {
    'direct_shows': 0,
    'direct_clicks': 0,
    'direct_ctr': 0.0,
    'premium_ctr': 0.0,
    'direct_min_cpc': 0.0,
    'direct_avg_cpc': 0.0,
    'direct_max_cpc': 0.0,
    'direct_recommended_cpc': 0.0,
    'direct_competition': 'unknown',
    'direct_first_place_bid': 0.0,
}


def add_empty_direct_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавление пустых колонок Direct если интеграция отключена.
    
    Args:
        df: DataFrame для обогащения
        
    Returns:
        DataFrame с добавленными пустыми колонками
    """
    for col, default_value in DIRECT_COLUMNS.items():
        if col not in df.columns:
            df[col] = default_value
            
    return df


def get_direct_columns() -> list:
    """
    Получение списка колонок Direct.
    
    Returns:
        Список названий колонок
    """
    return list(DIRECT_COLUMNS.keys())


