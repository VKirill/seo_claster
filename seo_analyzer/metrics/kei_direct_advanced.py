"""
Продвинутые KEI калькуляторы с данными Yandex Direct.

Включает метрики трафика, бюджета и качества.
"""

import pandas as pd


def kei_direct_traffic_potential(df: pd.DataFrame, target_position: int = 3) -> pd.Series:
    """
    Потенциал трафика при достижении целевой позиции.
    
    Формула: Shows * CTR(position) * MarketShare
    Прогноз кликов при выходе на target_position.
    
    Args:
        df: DataFrame с полями direct_shows, premium_ctr
        target_position: Целевая позиция (1-10)
        
    Returns:
        Series с прогнозируемым трафиком
    """
    shows = df.get('direct_shows', 0)
    premium_ctr = df.get('premium_ctr', 3)
    
    # CTR по позициям (примерные значения)
    position_ctr = {
        1: 1.0,    # 100% от premium_ctr
        2: 0.75,   # 75%
        3: 0.60,   # 60%
        4: 0.45,   # 45%
        5: 0.35,   # 35%
        6: 0.25,   # 25%
        7: 0.18,   # 18%
        8: 0.12,   # 12%
        9: 0.08,   # 8%
        10: 0.05   # 5%
    }
    
    ctr_factor = position_ctr.get(target_position, 0.60)
    effective_ctr = premium_ctr * ctr_factor
    
    return shows * (effective_ctr / 100)


def kei_direct_budget_required(df: pd.DataFrame, target_clicks: int = 100) -> pd.Series:
    """
    Требуемый бюджет для получения целевого количества кликов.
    
    Формула: target_clicks * recommended_cpc
    
    Args:
        df: DataFrame с полем direct_recommended_cpc
        target_clicks: Целевое количество кликов
        
    Returns:
        Series с требуемым бюджетом (руб)
    """
    cpc = df.get('direct_recommended_cpc', 100)
    return target_clicks * cpc


