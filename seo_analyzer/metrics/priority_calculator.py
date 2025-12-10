"""Расчет приоритетного скора и дополнительных метрик"""

import pandas as pd
import numpy as np


def ctr_potential(df: pd.DataFrame) -> pd.Series:
    """
    CTR Potential: Потенциальная кликабельность
    (ExactFreq / BaseFreq) * 100
    """
    exact_freq = df.get('frequency_exact', 0)
    base_freq = df.get('frequency_world', 0)
    
    return (exact_freq / (base_freq + 0.01)) * 100


def commercial_value(df: pd.DataFrame) -> pd.Series:
    """
    Commercial Value: Коммерческая ценность
    Freq * CommercialScore * (1 + isGeo) * (1 + isBrand)
    """
    freq = df.get('frequency_world', 0)
    commercial_score = df.get('commercial_score', 0)
    
    # Безопасное получение булевых колонок
    if 'has_geo' in df.columns:
        has_geo = df['has_geo'].astype(int)
    else:
        has_geo = 0
    
    if 'has_brand' in df.columns:
        has_brand = df['has_brand'].astype(int)
    else:
        has_brand = 0
    
    return freq * (1 + commercial_score / 10) * (1 + has_geo * 0.5) * (1 + has_brand * 0.3)


def traffic_potential(df: pd.DataFrame) -> pd.Series:
    """
    Traffic Potential: Потенциал трафика
    Frequency * (1 / (Difficulty + 1)) * CTR
    """
    freq = df.get('frequency_world', 0)
    difficulty = df.get('difficulty_score', 50)
    
    # Условный CTR в зависимости от сложности
    ctr = 0.3 * (100 - difficulty) / 100
    
    return freq * (1 / (difficulty / 100 + 1)) * ctr


def priority_score(
    df: pd.DataFrame,
    kei_effectiveness: pd.Series,
    priority_weights: dict
) -> pd.Series:
    """
    Priority Score: Итоговый приоритетный скор
    Взвешенная комбинация всех факторов
    """
    # Нормализуем компоненты
    freq_normalized = normalize_series(df.get('frequency_world', 0))
    
    difficulty = df.get('difficulty_score', 50)
    difficulty_normalized = 100 - difficulty  # Инвертируем (меньше = лучше)
    
    commercial_normalized = normalize_series(df.get('commercial_score', 0))
    
    kei_eff_normalized = normalize_series(kei_effectiveness)
    
    # Взвешенная сумма
    priority = (
        freq_normalized * priority_weights['frequency'] +
        difficulty_normalized * priority_weights['difficulty'] +
        commercial_normalized * priority_weights['commercial'] +
        kei_eff_normalized * priority_weights['kei_effectiveness']
    )
    
    return priority


def normalize_series(series: pd.Series) -> pd.Series:
    """Нормализация серии в диапазон 0-100"""
    min_val = series.min()
    max_val = series.max()
    
    if max_val - min_val == 0:
        return pd.Series([50] * len(series), index=series.index)
    
    normalized = ((series - min_val) / (max_val - min_val)) * 100
    return normalized


def normalize_column(series: pd.Series) -> pd.Series:
    """Нормализация колонки с обработкой NaN и Inf"""
    # Заменяем Inf на NaN
    series = series.replace([np.inf, -np.inf], np.nan)
    
    # Заполняем NaN медианой
    series = series.fillna(series.median())
    
    return normalize_series(series)

