"""
Domain Stats Updater
Обновление глобальной статистики доменов после анализа
"""

import pandas as pd
from typing import List, Dict, Any

from seo_analyzer.core.query_groups import GroupDatabaseManager
from seo_analyzer.core.domain_classifier import DomainClassifier


def update_global_domain_stats(
    df: pd.DataFrame,
    group_name: str,
    db_path = None  # Теперь всегда output/master_queries.db
):
    """
    Обновить глобальную статистику доменов после анализа группы
    
    Args:
        df: DataFrame с результатами анализа
        group_name: Название группы запросов
        db_path: Путь к БД (игнорируется, используется master_queries.db)
    """
    # Проверяем что DataFrame не пустой
    if len(df) == 0:
        print(f"📊 Обновление глобальной статистики доменов для группы '{group_name}'...")
        print("  ⚠️  DataFrame пустой, пропускаем обновление")
        return
    
    if 'serp_urls' not in df.columns:
        print("⚠️  Нет SERP данных для обновления статистики доменов")
        return
    
    print(f"📊 Обновление глобальной статистики доменов для группы '{group_name}'...")
    
    # Инициализируем менеджер БД (использует master_queries.db)
    from pathlib import Path
    db_path = Path("output/master_queries.db")
    db_manager = GroupDatabaseManager(query_group=group_name)
    
    # Инициализируем классификатор доменов
    domain_classifier = DomainClassifier(db_path=db_path)
    
    # Собираем данные по доменам
    domains_data = []
    
    for idx, row in df.iterrows():
        # Получаем SERP URLs
        serp_urls = row.get('serp_urls', [])
        if not serp_urls or not isinstance(serp_urls, list):
            continue
        
        # Определяем коммерциальность запроса
        is_commercial = _is_query_commercial(row)
        
        # Извлекаем домены из URL
        for url in serp_urls:
            domain = domain_classifier.extract_domain(url)
            if domain and domain not in ['', 'unknown']:
                domains_data.append({
                    'domain': domain,
                    'is_commercial': is_commercial
                })
    
    if not domains_data:
        print("  ⚠️  Нет данных для обновления")
        return
    
    # Пакетное обновление
    db_manager.batch_update_domains(domains_data, group_name)
    
    # Статистика
    unique_domains = len(set(d['domain'] for d in domains_data))
    print(f"  ✓ Обновлено: {unique_domains} уникальных доменов")
    print(f"  ✓ Всего наблюдений: {len(domains_data)}")


def _is_query_commercial(row: pd.Series) -> bool:
    """
    Определить коммерциальность запроса
    
    Args:
        row: Строка DataFrame
        
    Returns:
        True если запрос коммерческий
    """
    # Проверяем основной интент
    main_intent = row.get('main_intent', 'unknown')
    
    # Коммерческие интенты
    commercial_intents = ['commercial', 'commercial_geo', 'transactional']
    
    if main_intent in commercial_intents:
        return True
    
    # Проверяем скор коммерциализации
    commercial_score = row.get('commercial_score', 0)
    if commercial_score >= 5.0:  # Порог коммерциальности
        return True
    
    return False

