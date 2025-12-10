"""
Базовые KEI калькуляторы с данными Yandex Direct.

Включает метрики эффективности, прибыльности и конкуренции.
"""

import pandas as pd


def kei_direct_competition_score(df: pd.DataFrame) -> pd.Series:
    """
    Оценка конкуренции на основе данных Директа.
    
    Формула: (Frequency / Shows) * (100 / CPC)
    Высокий балл = низкая конкуренция и высокая частотность.
    
    Args:
        df: DataFrame с полями frequency_world, direct_shows, direct_avg_cpc
        
    Returns:
        Series с баллом конкуренции
    """
    freq = df.get('frequency_world', 0)
    shows = df.get('direct_shows', 0)
    cpc = df.get('direct_avg_cpc', 100)
    
    # Если показов нет - используем frequency как показы
    shows = shows.replace(0, freq)
    cpc = cpc.replace(0, 100)
    
    return (freq / shows) * (100 / cpc)

