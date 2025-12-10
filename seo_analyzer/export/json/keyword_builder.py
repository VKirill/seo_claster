"""Построитель информации о запросе для JSON"""

from typing import Dict
import pandas as pd
import numpy as np


class KeywordInfoBuilder:
    """Построитель информации о запросе"""
    
    # Список всех экспортируемых полей
    OPTIONAL_FIELDS = [
        'key_phrase',  # Главная фраза запроса
        'frequency_world',
        'frequency_exact',
        'main_intent',
        'funnel_stage',
        'difficulty_score',
        'pagerank_score',
        'priority_score',
        'target_page_type',
        'detected_brand',
        'is_brand_query',
        
        # География
        'has_geo',
        'geo_city',
        'geo_country',
        
        # Кластеры
        'semantic_cluster_id',
        'cluster_name',
        'word_match_cluster_id',
        'word_match_cluster_name',
        
        # KEI метрики (базовые)
        'kei_standard',
        'kei_effectiveness',
        'kei_competition',
        'kei_devaka',
        'kei_base_exact_ratio',
        'kei_coefficient',
        'kei_popularity',
        'kei_synergy',
        'kei_yandex_relevance',
        'kei_effectiveness_coefficient',
        
        # Yandex Direct данные
        'direct_shows',
        'direct_clicks',
        'direct_ctr',
        'premium_ctr',
        'first_place_clicks',
        'first_place_ctr',
        'premium_clicks',
        'direct_avg_cpc',
        'direct_min_cpc',
        'direct_max_cpc',
        'direct_recommended_cpc',
        'competition_level',
        'direct_first_place_bid',
        'direct_first_place_price',
        
        # KEI с Direct данными
        'kei_potential_traffic',
        'kei_cost_per_visit',
        'kei_direct_traffic_potential',
        'kei_direct_budget_required',
        
        # Бюджетирование
        'direct_monthly_budget',
        
        # SERP данные
        'serp_docs_count',
        'serp_main_pages',
        'serp_titles_count',
        'serp_commercial_domains',
        'serp_info_domains',
        
        # Ценовые данные SERP
        'serp_avg_price',
        'serp_min_price',
        'serp_max_price',
        'serp_median_price',
        'serp_currency',
        'serp_offers_count',
        'serp_offers_with_discount',
        'serp_avg_discount_percent',
        
        # Коммерческие признаки
        'commercial_score',
        'is_commercial',
        'is_wholesale',
        'is_urgent',
        
        # Потенциал
        'ctr_potential',
        'commercial_value',
        'traffic_potential',
        
        # Текстовая обработка
        'lemmatized',
        'words_count',
        
        # SEO рекомендации
        'suggested_url',
        'query_pattern',
    ]
    
    # Маппинг падежных форм
    CASE_MAPPING = {
        'form_nominative': 'nominative',      # Именительный (кто? что?)
        'form_genitive': 'genitive',          # Родительный (кого? чего?)
        'form_dative': 'dative',              # Дательный (кому? чему?)
        'form_accusative': 'accusative',      # Винительный (кого? что?)
        'form_instrumental': 'instrumental',  # Творительный (кем? чем?)
        'form_prepositional': 'prepositional', # Предложный (о ком? о чём?)
    }
    
    def build(self, row: pd.Series) -> Dict:
        """
        Строит информацию о запросе
        
        Args:
            row: Строка DataFrame
            
        Returns:
            Словарь с информацией о запросе
        """
        keyword_info = {'keyword': row.get('keyword', '')}
        
        # Добавляем опциональные поля
        self._add_optional_fields(keyword_info, row)
        
        # Добавляем падежные формы
        self._add_case_forms(keyword_info, row)
        
        return keyword_info
    
    def _add_optional_fields(self, keyword_info: Dict, row: pd.Series):
        """Добавляет опциональные поля"""
        for field in self.OPTIONAL_FIELDS:
            if field in row and not pd.isna(row[field]):
                value = row[field]
                keyword_info[field] = self._convert_value(value)
    
    def _convert_value(self, value):
        """Конвертирует numpy типы в native Python типы"""
        if isinstance(value, (np.integer, np.floating)):
            return float(value) if isinstance(value, np.floating) else int(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, str):
            return str(value)
        return value
    
    def _add_case_forms(self, keyword_info: Dict, row: pd.Series):
        """Добавляет падежные формы (если есть)"""
        case_forms = {}
        
        for df_field, case_name in self.CASE_MAPPING.items():
            if df_field in row and not pd.isna(row[df_field]):
                case_forms[case_name] = row[df_field]
        
        if case_forms:
            keyword_info['case_forms'] = case_forms

