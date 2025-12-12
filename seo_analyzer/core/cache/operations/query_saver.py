"""
Сохранение запросов в Master DB
"""

import sqlite3
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional


class QuerySaver:
    """Сохранение запросов в Master DB"""
    
    def __init__(self, db_path: Path, query_loader):
        """
        Args:
            db_path: Путь к базе данных
            query_loader: Экземпляр QueryLoader для загрузки существующих данных
        """
        self.db_path = db_path
        self.query_loader = query_loader
    
    def save_queries(
        self,
        group_name: str,
        df: pd.DataFrame,
        csv_path: Path = None,
        csv_hash: str = None
    ):
        """
        Сохраняет/обновляет запросы в master таблице
        
        Args:
            group_name: Название группы
            df: DataFrame со ВСЕМИ обработанными данными
            csv_path: Путь к CSV (опционально)
            csv_hash: Hash CSV (опционально)
        """
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()
            
            # Загружаем частоты из CSV файла если он указан (для автоматического восстановления)
            csv_frequencies = {}
            if csv_path and csv_path.exists():
                try:
                    from seo_analyzer.core.helpers import load_csv_data, normalize_dataframe_columns
                    csv_raw_df = load_csv_data(csv_path)
                    if not csv_raw_df.empty:
                        csv_normalized = normalize_dataframe_columns(csv_raw_df)
                        # Создаем словарь частот из CSV для быстрого поиска
                        if 'keyword' in csv_normalized.columns:
                            for idx, row in csv_normalized.iterrows():
                                keyword = row.get('keyword')
                                if keyword:
                                    csv_frequencies[keyword] = {
                                        'frequency_world': row.get('frequency_world', 0) if pd.notna(row.get('frequency_world')) else 0,
                                        'frequency_exact': row.get('frequency_exact', 0) if pd.notna(row.get('frequency_exact')) else 0
                                    }
                            
                            # Диагностика: проверяем частоты в CSV
                            csv_non_zero_world = sum(1 for v in csv_frequencies.values() if v.get('frequency_world', 0) > 0)
                            csv_non_zero_exact = sum(1 for v in csv_frequencies.values() if v.get('frequency_exact', 0) > 0)
                            print(f"  📂 CSV файл загружен: {len(csv_frequencies)} запросов")
                            print(f"  📊 Частоты в CSV: {csv_non_zero_world} с ненулевой частотой (мир), {csv_non_zero_exact} (точная)")
                except Exception as e:
                    # Если не удалось загрузить CSV - продолжаем без него
                    print(f"  ⚠️  Не удалось загрузить CSV для восстановления частот: {e}")
            elif csv_path:
                print(f"  ⚠️  CSV файл не найден: {csv_path}")
            
            # Загружаем существующие данные из БД перед сохранением
            existing_df = None
            try:
                existing_df = self.query_loader.load_queries(group_name)
                if existing_df is not None and len(existing_df) > 0:
                    existing_df = existing_df.set_index('keyword')
            except:
                existing_df = None
            
            # Маппинг имен колонок
            column_mapping = {
                'serp_docs_count': 'serp_found_docs',
                'serp_titles_count': 'serp_titles_with_keyword',
            }
            
            df_copy = df.copy()
            for df_col, db_col in column_mapping.items():
                if df_col in df_copy.columns and db_col not in df_copy.columns:
                    df_copy[db_col] = df_copy[df_col]
            
            # АВТОМАТИЧЕСКОЕ восстановление частот из CSV если они нулевые в DataFrame
            frequencies_from_csv_count = 0
            frequencies_world_from_csv = 0
            frequencies_exact_from_csv = 0
            
            if csv_frequencies:
                for idx, row in df_copy.iterrows():
                    keyword = row.get('keyword')
                    if keyword and keyword in csv_frequencies:
                        csv_freq_data = csv_frequencies[keyword]
                        
                        # Восстанавливаем частоты из CSV если в DataFrame они нулевые или отсутствуют
                        if 'frequency_world' in df_copy.columns:
                            df_freq_world = row.get('frequency_world', 0)
                            # Безопасное преобразование в скалярное значение
                            if isinstance(df_freq_world, pd.Series):
                                df_freq_world = df_freq_world.iloc[0] if len(df_freq_world) > 0 else 0
                            elif isinstance(df_freq_world, (list, tuple, np.ndarray)):
                                df_freq_world = df_freq_world[0] if len(df_freq_world) > 0 else 0
                            
                            csv_freq_world = csv_freq_data.get('frequency_world', 0)
                            # Безопасная проверка после преобразования в скаляр
                            if (pd.isna(df_freq_world) or df_freq_world == 0) and csv_freq_world > 0:
                                df_copy.at[idx, 'frequency_world'] = csv_freq_world
                                frequencies_from_csv_count += 1
                                frequencies_world_from_csv += 1
                        
                        if 'frequency_exact' in df_copy.columns:
                            df_freq_exact = row.get('frequency_exact', 0)
                            # Безопасное преобразование в скалярное значение
                            if isinstance(df_freq_exact, pd.Series):
                                df_freq_exact = df_freq_exact.iloc[0] if len(df_freq_exact) > 0 else 0
                            elif isinstance(df_freq_exact, (list, tuple, np.ndarray)):
                                df_freq_exact = df_freq_exact[0] if len(df_freq_exact) > 0 else 0
                            
                            csv_freq_exact = csv_freq_data.get('frequency_exact', 0)
                            # Безопасная проверка после преобразования в скаляр
                            if (pd.isna(df_freq_exact) or df_freq_exact == 0) and csv_freq_exact > 0:
                                df_copy.at[idx, 'frequency_exact'] = csv_freq_exact
                                frequencies_from_csv_count += 1
                                frequencies_exact_from_csv += 1
            
            if frequencies_from_csv_count > 0:
                print(f"  ✅ Автоматически восстановлено {frequencies_from_csv_count} частот из CSV файла")
                print(f"     - Частота (мир): {frequencies_world_from_csv} запросов")
                print(f"     - Частота (точная): {frequencies_exact_from_csv} запросов")
            
            # Объединяем существующие данные с новыми
            frequencies_restored_from_db_count = 0
            # ВАЖНО: Создаем индекс по keyword для быстрого поиска существующих данных
            existing_data_by_keyword = {}
            if existing_df is not None and len(existing_df) > 0:
                existing_df_indexed = existing_df.set_index('keyword')
                for idx, row in df_copy.iterrows():
                    keyword = row.get('keyword')
                    if keyword and keyword in existing_df_indexed.index:
                        existing_row = existing_df_indexed.loc[keyword]
                        existing_data_by_keyword[keyword] = existing_row
                        
                        # Сохраняем базовые данные (частоты) из БД если их нет в DataFrame
                        # ВАЖНО: Приоритеты:
                        # 1. Частоты из CSV файла (уже восстановлены выше)
                        # 2. Частоты из нового DataFrame (если они есть)
                        # 3. Частоты из БД (только если в новом DataFrame их нет)
                        basic_fields = ['frequency_world', 'frequency_exact']
                        for field in basic_fields:
                            if field in existing_df.columns and field in df_copy.columns:
                                existing_val = existing_row.get(field)
                                df_val = row.get(field)
                                
                                # Безопасная проверка для скалярных значений
                                # Если existing_val это Series или массив - берем первое значение
                                if isinstance(existing_val, pd.Series):
                                    existing_val = existing_val.iloc[0] if len(existing_val) > 0 else None
                                elif isinstance(existing_val, (list, tuple, np.ndarray)):
                                    existing_val = existing_val[0] if len(existing_val) > 0 else None
                                
                                # Безопасное преобразование df_val в скалярное значение
                                if isinstance(df_val, pd.Series):
                                    df_val = df_val.iloc[0] if len(df_val) > 0 else None
                                elif isinstance(df_val, (list, tuple, np.ndarray)):
                                    df_val = df_val[0] if len(df_val) > 0 else None
                                
                                # Логика приоритетов:
                                # 1. Если в df_copy уже есть ненулевая частота (из CSV или из DataFrame) - не трогаем
                                # 2. Если в df_copy частота нулевая/NaN, но в БД есть ненулевая - восстанавливаем из БД
                                # 3. Если в df_copy частота нулевая и в БД тоже нулевая - оставляем нулевую
                                if pd.notna(df_val) and df_val != 0:
                                    # В df_copy уже есть ненулевая частота - не перезаписываем
                                    pass
                                elif pd.notna(existing_val) and existing_val != 0:
                                    # В df_copy нет частоты или она нулевая, но в БД есть ненулевая - восстанавливаем
                                    df_copy.at[idx, field] = existing_val
                                    frequencies_restored_from_db_count += 1
                        
                        # Сохраняем SERP данные из БД если их нет в DataFrame
                        serp_fields = [
                            'serp_req_id', 'serp_status', 'serp_error_message',
                            'serp_found_docs', 'serp_main_pages_count', 'serp_titles_with_keyword',
                            'serp_commercial_domains', 'serp_info_domains',
                            'serp_intent', 'serp_confidence', 'serp_docs_with_offers',
                            'serp_total_docs', 'serp_offer_ratio',
                            'serp_avg_price', 'serp_min_price', 'serp_max_price',
                            'serp_median_price', 'serp_currency',
                            'serp_offers_count', 'serp_offers_with_discount',
                            'serp_avg_discount_percent', 'serp_top_urls', 'serp_lsi_phrases',
                            'serp_created_at', 'serp_updated_at'
                        ]
                        for field in serp_fields:
                            if field in existing_df.columns:
                                existing_val = existing_row.get(field)
                                df_val = row.get(field)
                                
                                # Безопасная проверка для скалярных значений
                                # Если existing_val это Series или массив - берем первое значение
                                if isinstance(existing_val, pd.Series):
                                    existing_val = existing_val.iloc[0] if len(existing_val) > 0 else None
                                elif isinstance(existing_val, (list, tuple, np.ndarray)):
                                    existing_val = existing_val[0] if len(existing_val) > 0 else None
                                
                                # Безопасное преобразование df_val в скалярное значение
                                if isinstance(df_val, pd.Series):
                                    df_val = df_val.iloc[0] if len(df_val) > 0 else None
                                elif isinstance(df_val, (list, tuple, np.ndarray)):
                                    df_val = df_val[0] if len(df_val) > 0 else None
                                
                                # Проверяем что поле существует в df_copy перед установкой
                                if field not in df_copy.columns:
                                    # Если поля нет - создаем его с None значениями
                                    df_copy[field] = None
                                
                                if pd.notna(existing_val):
                                    # ВАЖНО: Для serp_top_urls и serp_lsi_phrases проверяем пустые списки
                                    # Пустые списки не должны перезаписывать данные из БД!
                                    is_empty_list = isinstance(df_val, list) and len(df_val) == 0
                                    is_empty_str = isinstance(df_val, str) and df_val.strip() in ('', '[]', 'null', 'NULL', 'None')
                                    
                                    # Безопасная проверка df_val после преобразования в скаляр
                                    if pd.isna(df_val) or df_val == '' or df_val == 0 or is_empty_list or is_empty_str:
                                        # Для SERP полей (списки/JSON) - не перезаписываем если в БД есть данные
                                        if field in ('serp_top_urls', 'serp_lsi_phrases'):
                                            # Проверяем что в БД действительно есть данные (не пустые)
                                            if isinstance(existing_val, str):
                                                existing_val_str = existing_val.strip()
                                                if existing_val_str and existing_val_str not in ('', '[]', 'null', 'NULL', 'None'):
                                                    # В БД есть данные - не перезаписываем пустым значением
                                                    continue
                                            elif isinstance(existing_val, (list, tuple)) and len(existing_val) > 0:
                                                # В БД есть данные - не перезаписываем пустым значением
                                                continue
                                        
                                        # Дополнительная проверка: если existing_val все еще Series - конвертируем
                                        if isinstance(existing_val, pd.Series):
                                            existing_val = existing_val.iloc[0] if len(existing_val) > 0 else None
                                        elif isinstance(existing_val, (list, tuple, np.ndarray)):
                                            # Для списков берем весь список, а не первый элемент
                                            if field in ('serp_top_urls', 'serp_lsi_phrases'):
                                                # Для SERP полей сохраняем весь список
                                                df_copy.at[idx, field] = existing_val
                                                continue
                                            else:
                                                existing_val = existing_val[0] if len(existing_val) > 0 else None
                                        
                                        # Убеждаемся что это скалярное значение перед установкой
                                        if not isinstance(existing_val, (pd.Series, list, tuple, np.ndarray)):
                                            df_copy.at[idx, field] = existing_val
            
            # Обновляем метаданные группы
            if csv_path and csv_hash:
                cursor.execute('''
                    INSERT OR REPLACE INTO query_groups 
                    (group_name, csv_file_path, csv_hash, total_queries, unique_queries, 
                     duplicates_removed, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    group_name,
                    str(csv_path),
                    csv_hash,
                    len(df),
                    len(df),
                    0
                ))
            
            # Подготавливаем данные для вставки
            queries_data = []
            
            # Вспомогательная функция для безопасного получения значений
            def safe_get(row, key, default=None, cast=None):
                """Безопасное получение значения с приведением типа"""
                if key not in df_copy.columns:
                    return default
                val = row.get(key)
                if pd.isna(val):
                    return default
                if cast:
                    try:
                        return cast(val)
                    except (ValueError, TypeError):
                        return default
                return val
            
            for _, row in df_copy.iterrows():
                keyword = row.get('keyword')
                
                # SERP TOP URLs как JSON
                serp_top_urls = None
                if 'serp_top_urls' in df_copy.columns:
                    val = row.get('serp_top_urls')
                    
                    # ВАЖНО: Проверяем, есть ли данные в БД для этого запроса
                    existing_serp_data = None
                    if keyword and keyword in existing_data_by_keyword:
                        existing_row = existing_data_by_keyword[keyword]
                        if 'serp_top_urls' in existing_row.index:
                            existing_serp_data = existing_row.get('serp_top_urls')
                            # Безопасное преобразование Series в скаляр
                            if isinstance(existing_serp_data, pd.Series):
                                existing_serp_data = existing_serp_data.iloc[0] if len(existing_serp_data) > 0 else None
                    
                    # Если val это пустой список или пустая строка, но в БД есть данные - используем данные из БД
                    is_empty = False
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        is_empty = True
                    elif isinstance(val, list) and len(val) == 0:
                        is_empty = True
                    elif isinstance(val, str) and val.strip() in ('', '[]', 'null', 'NULL', 'None'):
                        is_empty = True
                    
                    if is_empty and existing_serp_data:
                        # Пустое значение, но в БД есть данные - используем данные из БД
                        if isinstance(existing_serp_data, str) and existing_serp_data.strip() not in ('', '[]', 'null', 'NULL', 'None'):
                            serp_top_urls = existing_serp_data
                        elif isinstance(existing_serp_data, (list, tuple)) and len(existing_serp_data) > 0:
                            serp_top_urls = json.dumps(existing_serp_data, ensure_ascii=False)
                    elif val is not None and not (isinstance(val, float) and pd.isna(val)):
                        # Есть данные в DataFrame - используем их
                        if isinstance(val, str):
                            serp_top_urls = val
                        elif isinstance(val, list) and len(val) > 0:
                            if isinstance(val[0], dict):
                                serp_top_urls = json.dumps(val, ensure_ascii=False)
                            elif isinstance(val[0], str):
                                normalized_urls = []
                                for i, url in enumerate(val[:20], 1):
                                    normalized_urls.append({
                                        'position': i,
                                        'url': url,
                                        'domain': '',
                                        'title': '',
                                        'is_commercial': False
                                    })
                                serp_top_urls = json.dumps(normalized_urls, ensure_ascii=False)
                            else:
                                serp_top_urls = json.dumps(val, ensure_ascii=False)
                        elif isinstance(val, list) and len(val) == 0:
                            # Пустой список - не сохраняем (оставляем NULL или данные из БД)
                            if existing_serp_data:
                                if isinstance(existing_serp_data, str) and existing_serp_data.strip() not in ('', '[]', 'null', 'NULL', 'None'):
                                    serp_top_urls = existing_serp_data
                                elif isinstance(existing_serp_data, (list, tuple)) and len(existing_serp_data) > 0:
                                    serp_top_urls = json.dumps(existing_serp_data, ensure_ascii=False)
                            # Иначе оставляем None (не сохраняем пустой список)
                        else:
                            serp_top_urls = json.dumps(val, ensure_ascii=False) if val else None
                elif 'serp_urls' in df_copy.columns:
                    val = row.get('serp_urls')
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        if isinstance(val, str):
                            try:
                                parsed = json.loads(val)
                                if isinstance(parsed, list) and len(parsed) > 0:
                                    if isinstance(parsed[0], str):
                                        normalized_urls = []
                                        for i, url in enumerate(parsed[:20], 1):
                                            normalized_urls.append({
                                                'position': i,
                                                'url': url,
                                                'domain': '',
                                                'title': '',
                                                'is_commercial': False
                                            })
                                        serp_top_urls = json.dumps(normalized_urls, ensure_ascii=False)
                                    else:
                                        serp_top_urls = val
                                else:
                                    serp_top_urls = val
                            except:
                                serp_top_urls = val
                        elif isinstance(val, list):
                            normalized_urls = []
                            for i, url in enumerate(val[:20], 1):
                                if isinstance(url, str):
                                    normalized_urls.append({
                                        'position': i,
                                        'url': url,
                                        'domain': '',
                                        'title': '',
                                        'is_commercial': False
                                    })
                                elif isinstance(url, dict):
                                    normalized_urls.append(url)
                            serp_top_urls = json.dumps(normalized_urls, ensure_ascii=False) if normalized_urls else None
                        else:
                            serp_top_urls = json.dumps(val, ensure_ascii=False) if val else None
                
                # LSI phrases как JSON
                serp_lsi_phrases = None
                if 'serp_lsi_phrases' in df_copy.columns:
                    val = row.get('serp_lsi_phrases')
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        serp_lsi_phrases = val if isinstance(val, str) else json.dumps(val)
                elif 'lsi_phrases' in df_copy.columns:
                    val = row.get('lsi_phrases')
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        serp_lsi_phrases = val if isinstance(val, str) else json.dumps(val)
                
                queries_data.append((
                    group_name,
                    safe_get(row, 'keyword', ''),
                    safe_get(row, 'frequency_world', 0, int),
                    safe_get(row, 'frequency_exact', 0, int),
                    safe_get(row, 'normalized'),
                    safe_get(row, 'lemmatized'),
                    safe_get(row, 'words_count', 0, int),
                    safe_get(row, 'main_words'),
                    safe_get(row, 'key_phrase'),
                    safe_get(row, 'ner_entities'),
                    safe_get(row, 'ner_locations'),
                    safe_get(row, 'has_geo', False, bool),
                    safe_get(row, 'geo_type'),
                    safe_get(row, 'geo_country'),
                    safe_get(row, 'geo_city'),
                    safe_get(row, 'main_intent'),
                    safe_get(row, 'commercial_score', 0.0, float),
                    safe_get(row, 'informational_score', 0.0, float),
                    safe_get(row, 'navigational_score', 0.0, float),
                    safe_get(row, 'is_commercial', False, bool),
                    safe_get(row, 'is_wholesale', False, bool),
                    safe_get(row, 'is_urgent', False, bool),
                    safe_get(row, 'is_diy', False, bool),
                    safe_get(row, 'is_review', False, bool),
                    safe_get(row, 'is_brand_query', False, bool),
                    safe_get(row, 'serp_query_hash'),
                    safe_get(row, 'serp_req_id'),
                    safe_get(row, 'serp_status') or 'completed',
                    safe_get(row, 'serp_error_message'),
                    safe_get(row, 'serp_found_docs', None, int),
                    safe_get(row, 'serp_main_pages_count', None, int),
                    safe_get(row, 'serp_titles_with_keyword', None, int),
                    safe_get(row, 'serp_commercial_domains', None, int),
                    safe_get(row, 'serp_info_domains', None, int),
                    safe_get(row, 'serp_created_at'),
                    safe_get(row, 'serp_updated_at'),
                    safe_get(row, 'serp_intent'),
                    safe_get(row, 'serp_confidence', 0.0, float),
                    safe_get(row, 'serp_docs_with_offers', 0, int),
                    safe_get(row, 'serp_total_docs', 0, int),
                    safe_get(row, 'serp_offer_ratio', 0.0, float),
                    safe_get(row, 'serp_avg_price', None, float),
                    safe_get(row, 'serp_min_price', None, float),
                    safe_get(row, 'serp_max_price', None, float),
                    safe_get(row, 'serp_median_price', None, float),
                    safe_get(row, 'serp_currency', 'RUR'),
                    safe_get(row, 'serp_offers_count', 0, int),
                    safe_get(row, 'serp_offers_with_discount', 0, int),
                    safe_get(row, 'serp_avg_discount_percent', None, float),
                    serp_top_urls,
                    serp_lsi_phrases,
                    safe_get(row, 'direct_shows', None, int),
                    safe_get(row, 'direct_clicks', None, int),
                    safe_get(row, 'direct_ctr', None, float),
                    safe_get(row, 'direct_min_cpc', None, float),
                    safe_get(row, 'direct_avg_cpc', None, float),
                    safe_get(row, 'direct_max_cpc', None, float),
                    safe_get(row, 'direct_recommended_cpc', None, float),
                    safe_get(row, 'direct_competition_level'),
                    safe_get(row, 'direct_first_place_bid', None, float),
                    safe_get(row, 'direct_first_place_price', None, float),
                    safe_get(row, 'kei', 0.0, float),
                    safe_get(row, 'difficulty', 0.0, float),
                    safe_get(row, 'competition_score', 0.0, float),
                    safe_get(row, 'potential_traffic', 0.0, float),
                    safe_get(row, 'expected_ctr', 0.0, float),
                    safe_get(row, 'detected_brand'),
                    safe_get(row, 'brand_confidence', 0.0, float),
                    safe_get(row, 'funnel_stage'),
                    safe_get(row, 'funnel_priority', 5, int),
                ))
            
            # Bulk insert или replace
            batch_size = 100
            total_batches = (len(queries_data) + batch_size - 1) // batch_size
            
            insert_query = '''
                INSERT OR REPLACE INTO master_queries 
                (group_name, keyword, frequency_world, frequency_exact,
                 normalized, lemmatized, words_count, main_words, key_phrase,
                 ner_entities, ner_locations,
                 has_geo, geo_type, geo_country, geo_city,
                 main_intent, commercial_score, informational_score, navigational_score,
                 is_commercial, is_wholesale, is_urgent, is_diy, is_review, is_brand_query,
                 serp_query_hash, serp_req_id, serp_status, serp_error_message,
                 serp_found_docs, serp_main_pages_count, serp_titles_with_keyword,
                 serp_commercial_domains, serp_info_domains, serp_created_at, serp_updated_at,
                 serp_intent, serp_confidence, serp_docs_with_offers, serp_total_docs, serp_offer_ratio,
                 serp_avg_price, serp_min_price, serp_max_price, serp_median_price, serp_currency,
                 serp_offers_count, serp_offers_with_discount, serp_avg_discount_percent,
                 serp_top_urls, serp_lsi_phrases,
                 direct_shows, direct_clicks, direct_ctr, direct_min_cpc, direct_avg_cpc,
                 direct_max_cpc, direct_recommended_cpc, direct_competition_level,
                 direct_first_place_bid, direct_first_place_price,
                 kei, difficulty, competition_score, potential_traffic, expected_ctr,
                 detected_brand, brand_confidence,
                 funnel_stage, funnel_priority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            saved_count = 0
            for i in range(0, len(queries_data), batch_size):
                batch = queries_data[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                try:
                    cursor.executemany(insert_query, batch)
                    saved_count += len(batch)
                    
                    if batch_num % 10 == 0 or batch_num == total_batches:
                        print(f"  💾 Сохранено {saved_count}/{len(queries_data)} запросов...")
                except Exception as e:
                    print(f"  ⚠️  Ошибка при сохранении батча {batch_num}: {e}")
                    raise
            
            conn.commit()
            
            # Диагностика сохранения частот
            if 'frequency_world' in df_copy.columns:
                non_zero_freq_world = (df_copy['frequency_world'] > 0).sum()
                total_rows = len(df_copy)
                print(f"  📊 Частоты в сохраняемых данных: {non_zero_freq_world} из {total_rows} с ненулевой частотой (мир)")
                if frequencies_from_csv_count > 0:
                    print(f"  ℹ️  Восстановлено {frequencies_from_csv_count} частот из CSV файла")
                if frequencies_restored_from_db_count > 0:
                    print(f"  ℹ️  Восстановлено {frequencies_restored_from_db_count} частот из существующих данных БД")
                if non_zero_freq_world == 0 and total_rows > 0:
                    print(f"  ⚠️  ВНИМАНИЕ: Все частоты равны нулю в сохраняемых данных!")
                    print(f"  ℹ️  Проверьте CSV файл на наличие колонок frequency_world и frequency_exact")
            
        finally:
            conn.close()

