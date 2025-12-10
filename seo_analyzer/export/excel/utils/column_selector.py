"""
Выбор колонок для экспорта в Excel
"""

from typing import List
import pandas as pd


def select_columns_for_export(df: pd.DataFrame) -> List[str]:
    """
    Выбрать колонки для экспорта
    
    Args:
        df: DataFrame с данными
        
    Returns:
        Список названий колонок для экспорта
    """
    # Проверяем наличие данных Direct
    has_direct_data = 'direct_shows' in df.columns and (df['direct_shows'] > 0).any()
    
    # Проверяем наличие частотности (Wordstat)
    has_frequency_data = 'frequency_exact' in df.columns and (df['frequency_exact'] > 0).any()
    
    # Порядок колонок согласно требованиям
    base_columns = [
        # Блок 1: Основная информация о кластере и запросе
        'cluster_name',              # Название кластера
        'semantic_cluster_id',       # ID кластера
        'keyword',                   # Запрос
        'lemmatized',                # lemmatized
        'cluster_lsi_phrases',       # LSI фразы кластера
        
        # Блок 2: Частотность и потенциал
        'frequency_world',           # Частота (мир)
        'frequency_exact',           # Частота (точная)
        'ctr_potential',             # Потенциал CTR
        
        # Блок 3: Классификация
        'main_intent',               # Основной интент
        'funnel_stage',              # funnel_stage
        'target_page_type',          # target_page_type
        
        # Блок 4: География
        'geo_type',                  # Тип локации
        'geo_city',                  # Город
        'geo_country',               # geo_country
        'geo_full_address',          # Полный адрес (для адресных запросов)
    ]
    
    # Фильтруем только существующие базовые колонки
    priority_columns = [col for col in base_columns if col in df.columns]
    
    # Блок 5: Yandex Direct колонки - только если есть данные Direct И частотность
    if has_direct_data and has_frequency_data:
        direct_columns = [
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
            'direct_monthly_budget',
        ]
        # Добавляем только существующие Direct колонки
        priority_columns.extend([col for col in direct_columns if col in df.columns])
    
    # Блок 6: Приоритет и KEI метрики
    kei_and_priority_columns = [
        'priority_score',            # Приоритет
        'kei_devaka',                # KEI Девака
        'kei_effectiveness',         # KEI Эффективность
        'kei_competition',           # KEI Конкуренция
        'kei_base_exact_ratio',      # KEI Соотношение
        'kei_coefficient',           # KEI Коэффициент
        'kei_popularity',            # KEI Популярность
        'kei_synergy',               # KEI Синергия
        'kei_effectiveness_coefficient',  # KEI Коэффициент эффективности
    ]
    priority_columns.extend([col for col in kei_and_priority_columns if col in df.columns])
    
    # Блок 7: Коммерческая ценность и SERP метрики
    value_and_serp_columns = [
        'commercial_value',          # Коммерческая ценность
        'serp_docs_count',           # Кол-во документов SERP
        'serp_titles_count',         # Кол-во заголовков
        'serp_main_pages_count',     # Кол-во главных
        'serp_internal_pages_count', # Кол-во внутренних
        # 'serp_commercial_domains', # СКРЫТО: не выводится в Excel
    ]
    priority_columns.extend([col for col in value_and_serp_columns if col in df.columns])
    
    # Блок 8: Перелинковка (В САМОМ КОНЦЕ)
    if 'related_clusters' in df.columns:
        priority_columns.append('related_clusters')
    
    # ИСКЛЮЧАЕМ из экспорта (но не удаляем из кода):
    # - Все остальные KEI метрики не из списка
    # - kei_standard, kei_yandex_relevance, kei_potential_traffic, kei_cost_per_visit
    # - kei_standard_normalized (уже удалено ранее)
    # - difficulty_score, words_count, suggested_url
    # - serp_main_pages, serp_info_domains, serp_commercial_domains
    # - commercial_score, is_commercial, is_wholesale, is_urgent, query_pattern
    
    return priority_columns

