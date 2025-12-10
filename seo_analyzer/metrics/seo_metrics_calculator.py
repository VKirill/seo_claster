"""
SEO Metrics Calculator Module (фасад для обратной совместимости)
Расчет KEI формул и SEO метрик
"""

import pandas as pd
from typing import Dict, Any

from .kei_calculator import kei_standard, kei_devaka, kei_base_exact_ratio
from .soltyk_calculator import (
    kei_soltyk_competition,
    kei_soltyk_effectiveness,
    kei_soltyk_coefficient,
    kei_soltyk_popularity,
    kei_soltyk_potential_traffic,
    kei_soltyk_cost_per_visit,
    kei_soltyk_potential_revenue,
    kei_soltyk_synergy,
    kei_soltyk_yandex_relevance
)
from .soltyk_advanced_calculator import kei_soltyk_effectiveness_coefficient
from .priority_calculator import (
    ctr_potential,
    commercial_value,
    traffic_potential,
    priority_score,
    normalize_column
)


class SEOMetricsCalculator:
    """
    Калькулятор всех KEI формул и SEO метрик
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.metrics
    """
    
    def __init__(self):
        # Конфигурация весов для приоритетного скора
        self.priority_weights = {
            'frequency': 0.3,
            'difficulty': 0.25,
            'commercial': 0.25,
            'kei_effectiveness': 0.20
        }
    
    def calculate_all_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Добавить все KEI колонки в DataFrame
        
        Required columns:
        - frequency_world (BaseFrequency)
        - frequency_exact (QuotePointFrequency) 
        - serp_docs_count (from SERP <found>)
        - serp_main_pages (MainPagesCount)
        - serp_titles_count (TitlesWithKeywordFoundCount)
        
        Optional columns:
        - yandex_direct_ctr
        - yandex_direct_budget
        - current_position
        - difficulty_score
        - commercial_score
        """
        print("  📊 Расчет стандартных KEI формул...")
        
        # Стандартные KEI формулы
        df['kei_standard'] = kei_standard(df)
        df['kei_devaka'] = kei_devaka(df)
        df['kei_base_exact_ratio'] = kei_base_exact_ratio(df)
        
        print("  📊 Расчет Soltyk KEI формул...")
        
        # Soltyk формулы (10 штук)
        df['kei_competition'] = kei_soltyk_competition(df)
        df['kei_effectiveness'] = kei_soltyk_effectiveness(df)
        df['kei_coefficient'] = kei_soltyk_coefficient(df)
        df['kei_popularity'] = kei_soltyk_popularity(df)
        df['kei_potential_traffic'] = kei_soltyk_potential_traffic(df)
        df['kei_cost_per_visit'] = kei_soltyk_cost_per_visit(df)
        df['kei_potential_revenue'] = kei_soltyk_potential_revenue(df)
        df['kei_synergy'] = kei_soltyk_synergy(df)
        df['kei_yandex_relevance'] = kei_soltyk_yandex_relevance(df)
        df['kei_effectiveness_coefficient'] = kei_soltyk_effectiveness_coefficient(df)
        # kei_standard_normalized - удалено
        
        print("  📊 Расчет дополнительных метрик...")
        
        # Дополнительные метрики
        df['ctr_potential'] = ctr_potential(df)
        df['commercial_value'] = commercial_value(df)
        df['traffic_potential'] = traffic_potential(df)
        
        # Итоговый приоритетный скор
        df['priority_score'] = priority_score(
            df,
            df['kei_effectiveness'],
            self.priority_weights
        )
        
        # Нормализуем некоторые метрики (0-100)
        df['kei_standard_normalized'] = normalize_column(df['kei_standard'])
        df['kei_effectiveness_normalized'] = normalize_column(df['kei_effectiveness'])
        df['priority_score_normalized'] = normalize_column(df['priority_score'])
        
        return df
    
    def get_top_queries_by_metric(
        self,
        df: pd.DataFrame,
        metric: str = 'priority_score',
        top_n: int = 100
    ) -> pd.DataFrame:
        """
        Получить топ запросов по метрике
        
        Args:
            df: DataFrame с метриками
            metric: Название метрики для сортировки
            top_n: Количество топ запросов
            
        Returns:
            DataFrame с топ запросами
        """
        if metric not in df.columns:
            raise ValueError(f"Metric '{metric}' not found in DataFrame")
        
        return df.nlargest(top_n, metric)
    
    def get_metrics_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Получить сводную статистику по метрикам
        
        Returns:
            Dict со статистикой
        """
        kei_columns = [col for col in df.columns if col.startswith('kei_')]
        
        summary = {
            'total_queries': len(df),
            'metrics_calculated': len(kei_columns),
            'average_metrics': {}
        }
        
        # Средние значения по всем KEI метрикам
        for col in kei_columns:
            if col in df.columns:
                summary['average_metrics'][col] = {
                    'mean': round(df[col].mean(), 2),
                    'median': round(df[col].median(), 2),
                    'std': round(df[col].std(), 2),
                    'min': round(df[col].min(), 2),
                    'max': round(df[col].max(), 2)
                }
        
        return summary
