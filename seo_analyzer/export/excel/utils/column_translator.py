"""
Перевод названий колонок на русский язык
"""

import sys
import importlib.util
from pathlib import Path


def _get_default_translation(col_name: str) -> str:
    """
    Базовый словарь переводов колонок на случай если backup файл недоступен
    
    Args:
        col_name: Английское название колонки
        
    Returns:
        Русское название колонки
    """
    translations = {
        # Основная информация
        'keyword': 'Запрос',
        'lemmatized': 'Лемматизированный',
        'cluster_name': 'Название кластера',
        'semantic_cluster_id': 'ID',
        'related_clusters': 'Связанные кластеры (перелинковка)',
        'cluster_lsi_phrases': 'LSI фразы кластера',
        
        # Частотность и потенциал
        'frequency_world': 'Частота (мир)',
        'frequency_exact': 'Частота (точная)',
        'ctr_potential': 'Потенциал CTR',
        
        # Классификация
        'main_intent': 'Основной интент',
        'funnel_stage': 'Этап воронки',
        'target_page_type': 'Тип целевой страницы',
        
        # География
        'geo_type': 'Тип локации',
        'geo_city': 'Город',
        'geo_country': 'Страна',
        'geo_street': 'Улица',
        'geo_house': 'Дом',
        'geo_full_address': 'Полный адрес',
        
        # Yandex Direct данные
        'direct_shows': 'Direct: Показы',
        'direct_clicks': 'Direct: Клики',
        'direct_ctr': 'Direct: CTR (%)',
        'premium_ctr': 'Direct: Premium CTR (%)',
        'first_place_clicks': 'Direct: Клики на 1-й позиции',
        'first_place_ctr': 'Direct: CTR 1-й позиции (%)',
        'premium_clicks': 'Direct: Premium клики',
        'direct_avg_cpc': 'Direct: CPC средний (₽)',
        'direct_min_cpc': 'Direct: CPC мин (₽)',
        'direct_max_cpc': 'Direct: CPC макс (₽)',
        'direct_recommended_cpc': 'Direct: Рекомендуемая ставка (₽)',
        'direct_competition': 'Direct: Конкуренция',
        'direct_first_place_bid': 'Direct: Ставка за 1 место (₽)',
        'direct_first_place_price': 'Direct: Цена 1-й позиции (₽)',
        'competition_level': 'Direct: Уровень конкуренции',
        'direct_monthly_budget': 'Direct: Месячный бюджет (₽)',
        
        # SEO метрики и приоритет
        'priority_score': 'Приоритет',
        'kei_devaka': 'KEI Девака',
        'kei_effectiveness': 'KEI Эффективность',
        'kei_competition': 'KEI Конкуренция',
        'kei_base_exact_ratio': 'KEI Соотношение',
        'kei_coefficient': 'KEI Коэффициент',
        'kei_popularity': 'KEI Популярность',
        'kei_synergy': 'KEI Синергия',
        'kei_effectiveness_coefficient': 'KEI Коэффициент эффективности',
        
        # SERP анализ
        'commercial_value': 'Коммерческая ценность',
        'serp_docs_count': 'Кол-во документов SERP',
        'serp_titles_count': 'Кол-во заголовков',
        'serp_commercial_domains': 'Коммерческих доменов в SERP',
        'serp_main_pages_count': 'Кол-во главных',
        'serp_internal_pages_count': 'Кол-во внутренних',
        
        # Дополнительные KEI метрики
        'kei_standard': 'KEI Стандартный',
        'kei_potential_traffic': 'KEI Потенциал трафика',
        'kei_cost_per_visit': 'KEI Цена визита',
        'kei_yandex_relevance': 'KEI Релевантность',
        'kei_direct_traffic_potential': 'KEI Direct: Потенциал трафика',
        'kei_direct_budget_required': 'KEI Direct: Бюджет на 100 кликов (₽)',
        
        # Сложность и конкуренция
        'difficulty_score': 'Сложность',
        'serp_main_pages': 'Основные страницы SERP',
        'serp_urls': 'URL из SERP',
        'cluster_common_urls': 'Общие URL кластера',
        'serp_info_domains': 'SERP: Инфо-домены',
        'commercial_score': 'Коммерческий скор',
        'is_commercial': 'Коммерческий',
        'is_wholesale': 'Оптовый',
        'is_urgent': 'Срочный',
        'query_pattern': 'Паттерн запроса',
        'words_count': 'Количество слов',
        'suggested_url': 'Рекомендуемый URL',
        
        # LSI
        'lsi_phrases': 'LSI фразы',
        'top_lsi_phrases': 'Топ LSI фразы',
        
        # Бренды
        'has_brand': 'Есть бренд',
        'brand_name': 'Название бренда',
        
        # Дополнительные поля
        'word_count': 'Количество слов',
        'is_question': 'Вопрос',
        'language': 'Язык',
    }
    
    return translations.get(col_name, col_name)


def get_column_translation(col_name: str) -> str:
    """
    Получить русское название для колонки
    
    Args:
        col_name: Английское название колонки
        
    Returns:
        Русское название колонки
    """
    # Импортируем функцию из backup файла для сохранения обратной совместимости
    backup_path = Path(__file__).parent.parent / 'data_writer.py.backup'
    if backup_path.exists():
        spec = importlib.util.spec_from_file_location("data_writer_backup", str(backup_path))
        if spec is not None and spec.loader is not None:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.get_column_translation(col_name)
    
    # Fallback: если не удалось загрузить из backup, возвращаем как есть или используем базовый словарь
    return _get_default_translation(col_name)

