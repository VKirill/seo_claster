"""
KEI метрики бюджетирования с Yandex Direct.

Расширенные расчёты бюджетов, прогнозы трафика и ROI
на основе реальных данных Direct API.
"""

import pandas as pd


def calculate_monthly_budget(df: pd.DataFrame, market_share: float = 5.0) -> pd.Series:
    """
    Расчёт месячного бюджета для запроса.
    
    Формула: Shows * (MarketShare / 100) * (CTR / 100) * CPC
    
    Args:
        df: DataFrame с полями direct_shows, premium_ctr, direct_avg_cpc
        market_share: Желаемая доля рынка (%)
        
    Returns:
        Series с месячным бюджетом (руб)
    """
    shows = df.get('direct_shows', 0)
    ctr = df.get('premium_ctr', 3)
    cpc = df.get('direct_avg_cpc', 100)
    
    # Расчёт ожидаемых показов и кликов
    target_impressions = shows * (market_share / 100)
    target_clicks = target_impressions * (ctr / 100)
    
    # Бюджет
    return target_clicks * cpc


def calculate_seasonality_factor(df: pd.DataFrame, 
                                 current_month: int,
                                 target_month: int) -> float:
    """
    Коэффициент сезонности для прогноза CPC.
    
    Простая модель: зимой (12,1,2) +20%, летом (6,7,8) -10%
    
    Args:
        df: DataFrame
        current_month: Текущий месяц (1-12)
        target_month: Целевой месяц (1-12)
        
    Returns:
        Коэффициент сезонности
    """
    # Сезонные коэффициенты
    winter_months = [12, 1, 2]
    summer_months = [6, 7, 8]
    
    current_factor = 1.0
    target_factor = 1.0
    
    if current_month in winter_months:
        current_factor = 1.2
    elif current_month in summer_months:
        current_factor = 0.9
        
    if target_month in winter_months:
        target_factor = 1.2
    elif target_month in summer_months:
        target_factor = 0.9
    
    return target_factor / current_factor

