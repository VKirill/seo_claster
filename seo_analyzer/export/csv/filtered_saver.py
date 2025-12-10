"""Сохранение отфильтрованных запросов обратно в файл"""

from pathlib import Path
import pandas as pd
from typing import Optional


def save_filtered_queries(
    df: pd.DataFrame, 
    original_file_path: Path,
    backup: bool = True
) -> bool:
    """
    Сохраняет отфильтрованные запросы обратно в исходный файл
    
    Это позволяет удалить мусорные запросы из файла после чистки,
    чтобы не фильтровать их каждый раз при следующем запуске.
    
    Args:
        df: DataFrame с отфильтрованными запросами
        original_file_path: Путь к исходному CSV файлу
        backup: Создавать ли бэкап файла перед перезаписью
        
    Returns:
        True если сохранение успешно, False в случае ошибки
        
    Example:
        >>> df = filter_queries(raw_df)
        >>> save_filtered_queries(df, Path("semantika/скуд.csv"))
        ✓ Создан бэкап: semantika/скуд_backup.csv
        ✓ Сохранено 450 запросов в: semantika/скуд.csv
    """
    try:
        original_file = Path(original_file_path)
        
        # Проверяем что файл существует
        if not original_file.exists():
            print(f"⚠️  Файл не найден: {original_file}")
            return False
        
        # Создаем бэкап перед перезаписью
        if backup:
            backup_path = original_file.parent / f"{original_file.stem}_backup{original_file.suffix}"
            
            # Если бэкап уже существует, не перезаписываем (защита от потери данных)
            if not backup_path.exists():
                import shutil
                shutil.copy2(original_file, backup_path)
                print(f"✓ Создан бэкап: {backup_path}")
            else:
                print(f"ℹ️  Бэкап уже существует: {backup_path}")
        
        # Определяем колонки для сохранения (только основные, без вычисленных)
        columns_to_save = _select_original_columns(df)
        
        # Создаем копию для сохранения
        df_to_save = df[columns_to_save].copy()
        
        # Сохраняем отфильтрованные запросы
        df_to_save.to_csv(
            original_file,
            index=False,
            encoding='utf-8-sig',
            sep=','
        )
        
        print(f"✓ Сохранено {len(df_to_save)} запросов в: {original_file}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")
        return False


def _select_original_columns(df: pd.DataFrame) -> list:
    """
    Выбирает только оригинальные колонки для сохранения
    
    Исключает вычисленные колонки (normalized, lemmatized, и т.д.)
    чтобы не засорять исходный файл.
    
    Args:
        df: DataFrame с данными
        
    Returns:
        Список колонок для сохранения
    """
    # Базовые колонки которые должны быть в файле
    base_columns = [
        'keyword',
        'frequency_world', 
        'frequency_exact',
    ]
    
    # Опциональные колонки (если есть в датафрейме)
    optional_columns = [
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
        'direct_competition',
        'direct_first_place_bid',
        'direct_first_place_price',
        
        # SERP данные
        'serp_urls',
        'serp_titles',
        'serp_docs_count',
        
        # География
        'geo_type',
        'geo_city',
        'geo_country',
        'geo_street',
        'geo_house',
        'geo_full_address',
        
        # Прочее
        'competition_level',
        'commercial_value',
    ]
    
    # Колонки которые НЕ нужно сохранять (вычисленные в процессе анализа)
    # Эти колонки хранятся в master_queries.db и загружаются оттуда
    exclude_columns = [
        'normalized',
        'lemmatized', 
        'words_count',
        'has_latin',
        'has_numbers',
        'main_words',
        'key_phrase',
        'ner_entities',
        'ner_locations',
        'main_intent',
        'funnel_stage',
        'target_page_type',
        'detected_brand',
        'is_brand_query',
        'semantic_cluster_id',
        'cluster_name',
        'cluster_lsi_phrases',
        'priority_score',
        'kei_devaka',
        'kei_effectiveness',
        'has_geo',
    ]
    
    # Собираем финальный список колонок
    columns_to_save = []
    
    for col in base_columns:
        if col in df.columns:
            columns_to_save.append(col)
    
    for col in optional_columns:
        if col in df.columns and col not in exclude_columns:
            columns_to_save.append(col)
    
    return columns_to_save

