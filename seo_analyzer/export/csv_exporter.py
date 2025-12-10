"""Экспорт результатов в CSV"""

from pathlib import Path
from typing import Dict, Optional
import pandas as pd


class CSVExporter:
    """Экспортер результатов в CSV"""
    
    def __init__(self, encoding: str = 'utf-8-sig'):
        """
        Инициализация
        
        Args:
            encoding: Кодировка файла
        """
        self.encoding = encoding
    
    def export_full_results(
        self,
        df: pd.DataFrame,
        output_path: Path,
        include_forms: bool = True
    ) -> bool:
        """
        Экспортирует полные результаты анализа
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            include_forms: Включать ли падежные формы
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт в CSV: {output_path.name}...")
            
            # Определяем колонки для экспорта
            columns_to_export = self._get_export_columns(df, include_forms)
            
            # Фильтруем только существующие колонки
            available_columns = [col for col in columns_to_export if col in df.columns]
            
            export_df = df[available_columns].copy()
            
            # Конвертируем списки в строки для CSV
            if 'serp_urls' in export_df.columns:
                export_df['serp_urls'] = export_df['serp_urls'].apply(
                    lambda x: ', '.join(x[:30]) if isinstance(x, list) and x else ''  # Все 30 URL
                )
            if 'lsi_phrases' in export_df.columns:
                export_df['lsi_phrases'] = export_df['lsi_phrases'].apply(
                    lambda x: ', '.join(x[:20]) if isinstance(x, list) and x else ''
                )
            
            # Сохраняем с кавычками для всех полей
            export_df.to_csv(
                output_path, 
                index=False, 
                encoding=self.encoding,
                sep=';',  # Используем точку с запятой как разделитель
                quoting=1,  # QUOTE_ALL - все поля в кавычках
                quotechar='"'  # Двойные кавычки
            )
            
            print(f"✓ Экспортировано {len(export_df)} запросов в {output_path}")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта CSV: {e}")
            return False
    
    def _get_export_columns(self, df: pd.DataFrame, include_forms: bool = True) -> list:
        """
        Возвращает список колонок для экспорта
        
        Args:
            df: DataFrame
            include_forms: Включать ли падежные формы
            
        Returns:
            Список названий колонок
        """
        base_columns = [
            # === САМОЕ ВАЖНОЕ для работы ===
            'keyword',                      # Исходный запрос
            'frequency_world',              # Частотность
            'frequency_exact',              # Точная частотность
            
            # === КЛАСТЕРИЗАЦИЯ ===
            'semantic_cluster_id',          # Группа/кластер (семантический)
            'cluster_name',                 # Название группы
            'related_clusters',             # Связанные кластеры (для перелинковки)
            'word_match_cluster_id',        # Группа по совпадениям слов
            'word_match_cluster_name',      # Название группы (KeyCollector)
            'topic_id',                     # Тема
            'topic_name',                   # Название темы
            
            # === ИНТЕНТ И ТИП ===
            'main_intent',                  # Интент (commercial/info/...)
            'funnel_stage',                 # Этап воронки
            'target_page_type',             # Тип страницы
            
            # === БРЕНДЫ ===
            'detected_brand',               # Найденный бренд
            'is_brand_query',               # Брендовый?
            
            # === ГЕО ===
            'has_geo',                      # Есть гео?
            'geo_type',                     # Тип (city/address/region)
            'geo_city',                     # Город
            'geo_country',                  # Страна
            'geo_street',                   # Улица (для адресов)
            'geo_house',                    # Дом (для адресов)
            'geo_full_address',             # Полный адрес
            
            # === ДОПОЛНИТЕЛЬНО ===
            'lemmatized',                   # Лемматизированный
            'words_count',                  # Количество слов
            'difficulty_score',             # Сложность продвижения
            'suggested_url',                # Предлагаемый URL
            
            # === KEI МЕТРИКИ ===
            'priority_score',               # Приоритетный скор
            'kei_effectiveness',            # KEI эффективность
            'kei_standard',                 # Стандартный KEI
            'kei_competition',              # KEI конкуренция
            'kei_coefficient',              # KEI коэффициент
            'kei_popularity',               # KEI популярность
            'kei_potential_traffic',        # KEI потенциал трафика
            # kei_cost_per_visit - НЕ добавляем (нет точных данных без Direct)
            'kei_synergy',                  # Синергия
            'kei_yandex_relevance',         # Yandex релевантность
            'kei_effectiveness_coefficient', # KEI Коэффициент эффективности
            'kei_standard_normalized',      # KEI Standard нормализованный
            'ctr_potential',                # CTR потенциал
            'commercial_value',             # Коммерческая ценность
            'traffic_potential',            # Потенциал трафика
            
            # === SERP МЕТРИКИ ===
            'serp_docs_count',              # Найдено документов
            'serp_main_pages',              # Главных страниц
            'serp_titles_count',            # Title с КС
            'serp_commercial_domains',      # Коммерческих доменов
            'serp_info_domains',            # Информационных доменов
            
            # === LSI ФРАЗЫ И SERP URL ===
            'cluster_lsi_phrases_str',      # LSI фразы кластера (топ-30, строка)
            'serp_urls',                    # SERP URL конкретного запроса (индивидуальные)
            'cluster_common_urls',          # Общие SERP URL кластера (топ-10)
            'all_topics_str',               # Все темы (soft clustering)
            
            # === ДЕТАЛЬНЫЕ ФЛАГИ (опционально) ===
            'commercial_score',
            'is_commercial',
            'is_wholesale',
            'is_urgent',
            'query_pattern',
            
            # === ИЕРАРХИЯ (если есть) ===
            'hierarchical_level1',
            'hierarchical_level2',
            'hierarchical_level3',
            'difficulty_level',
            'difficulty_cluster',
            
            # Граф
            'pagerank_score',
            'node_degree',
        ]
        
        # Проверяем наличие данных Direct
        has_direct_data = 'direct_shows' in df.columns and (df['direct_shows'] > 0).any()
        
        # Yandex Direct колонки - только если есть данные
        if has_direct_data:
            base_columns.extend([
                # Данные Direct
                'direct_shows',
                'direct_clicks',
                'direct_ctr',
                'premium_ctr',
                'direct_avg_cpc',
                'direct_min_cpc',
                'direct_max_cpc',
                'direct_recommended_cpc',
                'direct_competition',
                'direct_first_place_bid',
                
                # KEI метрики с Direct
                'kei_direct_traffic_potential',
                'kei_direct_budget_required',
                
                # Бюджетирование Direct
                'direct_monthly_budget',
            ])
        
        if include_forms:
            form_columns = [
                'form_nominative',
                'form_genitive',
                'form_dative',
                'form_accusative',
                'form_instrumental',
                'form_prepositional',
            ]
            base_columns.extend(form_columns)
        
        return base_columns
    
    def export_clusters_summary(
        self,
        df: pd.DataFrame,
        output_path: Path,
        cluster_column: str = 'semantic_cluster_id'
    ) -> bool:
        """
        Экспортирует сводку по кластерам
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            cluster_column: Колонка с ID кластера
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт сводки кластеров: {output_path.name}...")
            
            # Группируем по кластерам
            agg_dict = {
                'keyword': ['count', lambda x: x.iloc[0]],  # Количество и пример
                'frequency_world': 'sum',
            }
            
            # Добавляем difficulty_score только если есть
            if 'difficulty_score' in df.columns:
                agg_dict['difficulty_score'] = 'mean'
            
            cluster_summary = df.groupby(cluster_column).agg(agg_dict).reset_index()
            
            # Названия колонок
            columns = ['cluster_id', 'queries_count', 'example_query', 'total_frequency']
            if 'difficulty_score' in df.columns:
                columns.append('avg_difficulty')
            
            cluster_summary.columns = columns
            
            # Сохраняем
            cluster_summary.to_csv(output_path, index=False, encoding=self.encoding)
            
            print(f"✓ Экспортировано {len(cluster_summary)} кластеров")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта сводки: {e}")
            return False
    
    def export_top_queries(
        self,
        df: pd.DataFrame,
        output_path: Path,
        top_n: int = 1000,
        sort_by: str = 'frequency_world'
    ) -> bool:
        """
        Экспортирует топ запросов
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            top_n: Количество запросов
            sort_by: Колонка для сортировки
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт топ-{top_n} запросов: {output_path.name}...")
            
            if sort_by in df.columns:
                top_df = df.nlargest(top_n, sort_by)
            else:
                top_df = df.head(top_n)
            
            top_df.to_csv(output_path, index=False, encoding=self.encoding)
            
            print(f"✓ Экспортировано {len(top_df)} запросов")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта топа: {e}")
            return False

