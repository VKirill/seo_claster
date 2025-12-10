"""Продвинутые Soltyk KEI формулы"""

import pandas as pd
from .soltyk_calculator import kei_soltyk_effectiveness, kei_soltyk_competition


def kei_soltyk_effectiveness_coefficient(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 10: Коэффициент эффективности ключевой фразы (максимизировать)
    
    Формула: (Effectiveness²) / (1 + (Competition / 1000))
    
    Комплексная формула, объединяющая оценку эффективности и конкуренции
    для более точного анализа ключевых слов.
    
    Компоненты:
    - Effectiveness = ((ExactFreq / 12) + ExactFreq) / (2 + MainPages + TitlesCount)
    - Competition = ((1 + MainPages*2 + TitlesCount) * (DocsCount / ExactFreq)) / (1 + ExactFreq/12)
    
    Особенности:
    - Effectiveness возводится в квадрат для усиления влияния высокоэффективных запросов
    - Competition делится на 1000 для нормализации (снижает влияние высококонкурентных ниш)
    - Чем выше значение, тем лучше запрос для продвижения
    
    Применение:
    - Используется для поиска запросов с максимальным значением (MAX)
    - Помогает найти баланс между эффективностью и уровнем конкуренции
    - Эффективные запросы получают больший вес за счет возведения в квадрат
    
    Args:
        df: DataFrame с данными запросов
        
    Returns:
        Series с коэффициентом эффективности (максимизировать)
        
    Example:
        >>> df['kei_eff_coef'] = kei_soltyk_effectiveness_coefficient(df)
        >>> top_queries = df.nlargest(10, 'kei_eff_coef')
    """
    effectiveness = kei_soltyk_effectiveness(df)
    competition = kei_soltyk_competition(df)
    
    # Effectiveness² / (1 + Competition/1000)
    numerator = effectiveness ** 2
    denominator = 1 + (competition / 1000)
    
    return numerator / denominator


