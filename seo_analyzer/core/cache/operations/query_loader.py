"""
Загрузка запросов из Master DB
"""

import sqlite3
import json
import pandas as pd
from pathlib import Path
from typing import Optional

from seo_analyzer.core.serp.serp_data_normalizer import SERPDataNormalizer


class QueryLoader:
    """Загрузчик запросов из Master DB"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def load_queries(
        self,
        group_name: str,
        include_serp_urls: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Загружает ВСЕ данные по запросам из мастер-таблицы
        
        Args:
            group_name: Название группы
            include_serp_urls: Включать ли serp_top_urls (большие данные)
            
        Returns:
            DataFrame со всеми полями или None
        """
        conn = sqlite3.connect(self.db_path)
        
        # Выбираем все колонки кроме id и timestamps
        columns = """
            keyword, frequency_world, frequency_exact,
            normalized, lemmatized, words_count, main_words, key_phrase,
            ner_entities, ner_locations,
            has_geo, geo_type, geo_country, geo_city,
            main_intent, commercial_score, informational_score, navigational_score,
            is_commercial, is_wholesale, is_urgent, is_diy, is_review, is_brand_query,
            serp_query_hash, serp_found_docs, serp_main_pages_count, serp_titles_with_keyword,
            serp_commercial_domains, serp_info_domains, serp_created_at,
            serp_intent, serp_confidence, serp_docs_with_offers, serp_total_docs, serp_offer_ratio,
            serp_avg_price, serp_min_price, serp_max_price, serp_median_price, serp_currency,
            serp_offers_count, serp_offers_with_discount, serp_avg_discount_percent,
            {serp_urls}
            serp_lsi_phrases,
            direct_shows, direct_clicks, direct_ctr, direct_min_cpc, direct_avg_cpc,
            direct_max_cpc, direct_recommended_cpc, direct_competition_level,
            direct_first_place_bid, direct_first_place_price,
            kei, difficulty, competition_score, potential_traffic, expected_ctr,
            detected_brand, brand_confidence,
            funnel_stage, funnel_priority
        """.format(serp_urls='serp_top_urls,' if include_serp_urls else '')
        
        query = f'''
            SELECT {columns}
            FROM master_queries
            WHERE group_name = ?
            ORDER BY frequency_world DESC
        '''
        
        df = pd.read_sql_query(query, conn, params=(group_name,))
        conn.close()
        
        if df.empty:
            return None
        
        # Добавляем алиасы для совместимости с кодом
        if 'serp_found_docs' in df.columns:
            df['serp_docs_count'] = df['serp_found_docs']
        if 'serp_main_pages_count' in df.columns:
            df['serp_main_pages'] = df['serp_main_pages_count']
            df['serp_internal_pages_count'] = (
                df['serp_docs_count'] - df['serp_main_pages_count']
            ).fillna(0).clip(lower=0).astype(int)
        if 'serp_titles_with_keyword' in df.columns:
            df['serp_titles_count'] = df['serp_titles_with_keyword']
        
        # Нормализуем serp_top_urls в единый формат
        if 'serp_top_urls' in df.columns:
            def normalize_serp_urls(val):
                """Нормализует SERP URLs в единый формат"""
                # Обработка NULL/NaN значений
                if pd.isna(val) or val is None:
                    return []
                if isinstance(val, str) and (val.strip() == '' or val.strip().lower() == 'null'):
                    return []
                normalized = SERPDataNormalizer.normalize_serp_urls(val)
                return normalized
            
            df['serp_top_urls'] = df['serp_top_urls'].apply(normalize_serp_urls)
            
            # Диагностика нормализации
            serp_with_urls = df['serp_top_urls'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum()
            serp_empty = len(df) - serp_with_urls
            serp_null_count = df['serp_top_urls'].isna().sum() if 'serp_top_urls' in df.columns else 0
            print(f"   ✓ SERP URLs: {serp_with_urls} запросов с URL, {serp_empty} без URL")
            if serp_null_count > 0:
                print(f"   ⚠️  NULL значений: {serp_null_count}")
        
        # Преобразуем serp_lsi_phrases в lsi_phrases если нужно
        if 'serp_lsi_phrases' in df.columns and 'lsi_phrases' not in df.columns:
            def parse_lsi_phrases(val):
                if pd.isna(val) or val is None or val == '':
                    return []
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if isinstance(parsed, list):
                            result = []
                            for item in parsed:
                                if isinstance(item, str):
                                    result.append({'phrase': item, 'frequency': 1, 'source': 'unknown'})
                                elif isinstance(item, dict):
                                    result.append(item)
                            return result
                        return parsed if isinstance(parsed, list) else []
                    except (json.JSONDecodeError, TypeError):
                        return []
                elif isinstance(val, list):
                    return val
                return []
            
            df['lsi_phrases'] = df['serp_lsi_phrases'].apply(parse_lsi_phrases)
            
            # Диагностика LSI фраз
            lsi_non_empty = df['lsi_phrases'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum()
            lsi_empty = len(df) - lsi_non_empty
            print(f"   ✓ LSI фразы: {lsi_non_empty} запросов с LSI, {lsi_empty} без LSI")
        
        print(f"📦 Master DB: загружено {len(df)} запросов для группы '{group_name}'")
        print(f"   ✓ Интент: {df['main_intent'].notna().sum()} записей")
        print(f"   ✓ SERP: {df['serp_found_docs'].notna().sum()} записей")
        print(f"   ✓ Direct: {df['direct_shows'].notna().sum()} записей")
        
        # Диагностика частот при загрузке из БД
        if 'frequency_world' in df.columns:
            non_zero_freq_world = (df['frequency_world'] > 0).sum()
            total_rows = len(df)
            print(f"   ✓ Частота (мир): {non_zero_freq_world} из {total_rows} запросов с ненулевой частотой")
            if non_zero_freq_world == 0 and total_rows > 0:
                print(f"   ⚠️  ВНИМАНИЕ: Все частоты (мир) равны нулю в БД!")
                # Проверяем типы данных
                print(f"   ℹ️  Тип данных frequency_world: {df['frequency_world'].dtype}")
                print(f"   ℹ️  Примеры значений: {df['frequency_world'].head(10).tolist()}")
        
        if 'frequency_exact' in df.columns:
            non_zero_freq_exact = (df['frequency_exact'] > 0).sum()
            total_rows = len(df)
            print(f"   ✓ Частота (точная): {non_zero_freq_exact} из {total_rows} запросов с ненулевой частотой")
            if non_zero_freq_exact == 0 and total_rows > 0:
                print(f"   ⚠️  ВНИМАНИЕ: Все частоты (точная) равны нулю в БД!")
                # Проверяем типы данных
                print(f"   ℹ️  Тип данных frequency_exact: {df['frequency_exact'].dtype}")
                print(f"   ℹ️  Примеры значений: {df['frequency_exact'].head(10).tolist()}")
        
        return df

