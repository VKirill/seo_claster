"""Построитель информации Yandex Direct для кластера"""

from typing import Dict
import pandas as pd


class DirectInfoBuilder:
    """Добавляет агрегированные данные Yandex Direct"""
    
    def add(self, cluster_info: Dict, df: pd.DataFrame):
        """
        Добавляет информацию Yandex Direct в cluster_info
        
        Args:
            cluster_info: Словарь с информацией о кластере
            df: DataFrame кластера
        """
        if 'direct_shows' not in df.columns:
            return
        
        # Берем только запросы с данными Direct (показы > 0)
        direct_df = df[df['direct_shows'] > 0]
        
        if len(direct_df) == 0:
            return
        
        # Создаем объект с данными Direct
        direct_info = {}
        
        # Агрегированные показы и клики (сумма)
        direct_info['total_shows'] = int(pd.to_numeric(direct_df['direct_shows'], errors='coerce').sum())
        direct_info['total_clicks'] = int(pd.to_numeric(direct_df['direct_clicks'], errors='coerce').sum())
        
        # Средние метрики
        direct_info['avg_ctr'] = round(pd.to_numeric(direct_df['direct_ctr'], errors='coerce').mean(), 2)
        direct_info['avg_cpc'] = round(pd.to_numeric(direct_df['direct_avg_cpc'], errors='coerce').mean(), 2)
        
        # Диапазон CPC
        if 'direct_min_cpc' in df.columns:
            direct_info['min_cpc'] = round(pd.to_numeric(direct_df['direct_min_cpc'], errors='coerce').min(), 2)
        if 'direct_max_cpc' in df.columns:
            direct_info['max_cpc'] = round(pd.to_numeric(direct_df['direct_max_cpc'], errors='coerce').max(), 2)
        
        # Средняя конкуренция
        if 'direct_competition' in df.columns:
            direct_info['avg_competition'] = round(pd.to_numeric(direct_df['direct_competition'], errors='coerce').mean(), 2)
        
        # Бюджет на месяц (сумма по всем запросам кластера)
        if 'direct_monthly_budget' in df.columns:
            monthly_budgets = pd.to_numeric(direct_df['direct_monthly_budget'], errors='coerce').dropna()
            if len(monthly_budgets) > 0:
                direct_info['total_monthly_budget'] = round(monthly_budgets.sum(), 2)
        
        # Количество запросов с данными Direct
        direct_info['queries_with_direct_data'] = len(direct_df)
        direct_info['direct_coverage'] = round(len(direct_df) / len(df) * 100, 1)
        
        # Добавляем в cluster_info
        cluster_info['yandex_direct'] = direct_info

