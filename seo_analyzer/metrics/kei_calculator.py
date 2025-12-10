"""Стандартные KEI формулы"""

import pandas as pd


def kei_standard(df: pd.DataFrame) -> pd.Series:
    """
    KEI Standard: (Freq/2)^2 / Competition
    Классическая формула KEI
    """
    freq = df.get('frequency_world', 0)
    competition = df.get('serp_docs_count', 0)
    
    return ((freq / 2) ** 2) / (competition + 1)


def kei_devaka(df: pd.DataFrame) -> pd.Series:
    """
    KEI Devaka: (ExactFreq * 3 + BaseFreq) / BaseFreq
    Показывает специфичность запроса
    """
    base_freq = df.get('frequency_world', 0)
    exact_freq = df.get('frequency_exact', 0)
    
    return (exact_freq * 3 + base_freq) / (base_freq + 0.01)


def kei_base_exact_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Base/Exact Ratio: BaseFreq / ExactFreq
    Показывает широту запроса
    """
    base_freq = df.get('frequency_world', 0)
    exact_freq = df.get('frequency_exact', 0)
    
    return base_freq / (exact_freq + 0.01)

