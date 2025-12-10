"""
Интегратор Yandex Direct в основной пайплайн анализа.

Добавляет данные Директа в DataFrame с запросами и кластерами.
"""

import pandas as pd
import logging
from typing import Optional, List

from .yandex_direct_enricher import YandexDirectEnricher
from .yandex_direct_schema import add_empty_direct_columns


logger = logging.getLogger(__name__)


class YandexDirectIntegrator:
    """Интеграция данных Yandex Direct в основной пайплайн."""
    
    def __init__(self, token: str, use_sandbox: bool = False, geo_id: int = 213,
                 enabled: bool = True, minus_words_file: str = "yandex_direct_minus_words.txt",
                 db_path: Optional[str] = None):
        """
        Инициализация.
        
        Args:
            token: OAuth токен Yandex Direct
            use_sandbox: Использовать sandbox API
            geo_id: ID региона (213 = Москва)
            enabled: Включить интеграцию
            minus_words_file: Путь к файлу с минус-словами
            db_path: Путь к БД (по умолчанию output/serp_data.db)
        """
        self.enabled = enabled
        if self.enabled and token:
            self.enricher = YandexDirectEnricher(token, use_sandbox, geo_id, minus_words_file, db_path)
        else:
            self.enricher = None
            
    def enrich_dataframe(self, df: pd.DataFrame, query_column: str = 'query') -> pd.DataFrame:
        """
        Обогащение DataFrame данными из Yandex Direct.
        
        Добавляет колонки:
        - direct_shows: показы за последние 30 дней
        - direct_clicks: клики
        - direct_ctr: CTR в поиске
        - direct_premium_ctr: CTR на премиум позициях
        - direct_min_cpc: минимальный CPC
        - direct_avg_cpc: средний CPC
        - direct_max_cpc: максимальный CPC
        - direct_recommended_cpc: рекомендуемая ставка
        - direct_competition: уровень конкуренции (low/medium/high)
        - direct_first_place_bid: ставка за 1 место
        
        Args:
            df: DataFrame с запросами
            query_column: Название колонки с запросами
            
        Returns:
            DataFrame с добавленными колонками
        """
        if not self.enabled or self.enricher is None:
            logger.info("Yandex Direct integration disabled")
            return add_empty_direct_columns(df)
            
        if df.empty:
            print(f"⚠️  DataFrame пустой, пропускаем обогащение Yandex Direct")
            return add_empty_direct_columns(df)
            
        if query_column not in df.columns:
            print(f"⚠️  Колонка '{query_column}' не найдена в DataFrame")
            print(f"   Доступные колонки: {', '.join(df.columns.tolist()[:10])}")
            return add_empty_direct_columns(df)
            
        queries = df[query_column].unique().tolist()
        logger.info(f"Enriching {len(queries)} unique queries with Yandex Direct data")
        
        try:
            # Получение данных из Директа
            direct_data = self.enricher.enrich_queries(queries, use_cache=True)
            
            # Создание DataFrame с данными Директа
            direct_df = pd.DataFrame([
                {
                    query_column: query,
                    'direct_shows': data.get('shows', 0),
                    'direct_clicks': data.get('clicks', 0),
                    'direct_ctr': data.get('ctr', 0.0),
                    'premium_ctr': data.get('premium_ctr', 0.0),
                    'direct_min_cpc': data.get('min_cpc', 0.0),
                    'direct_avg_cpc': data.get('avg_cpc', 0.0),
                    'direct_max_cpc': data.get('max_cpc', 0.0),
                    'direct_recommended_cpc': data.get('recommended_cpc', 0.0),
                    'direct_competition': data.get('competition_level', 'unknown'),
                    'direct_first_place_bid': data.get('first_place_bid', 0.0),
                }
                for query, data in direct_data.items()
            ])
            
            # Мердж с основным DataFrame
            result_df = df.merge(direct_df, on=query_column, how='left')
            
            # Заполнение пустых значений
            direct_columns = [col for col in result_df.columns if col.startswith('direct_') or col == 'premium_ctr']
            for col in direct_columns:
                if result_df[col].dtype in ['float64', 'int64']:
                    result_df[col] = result_df[col].fillna(0)
                else:
                    result_df[col] = result_df[col].fillna('unknown')
                    
            logger.info("Successfully enriched DataFrame with Yandex Direct data")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to enrich DataFrame with Yandex Direct: {e}")
            return add_empty_direct_columns(df)
            
    def enrich_clusters(self, clusters: List[dict]) -> List[dict]:
        """
        Обогащение списка кластеров.
        
        Args:
            clusters: Список кластеров с полем 'queries'
            
        Returns:
            Кластеры с добавленными полями direct_*
        """
        if not self.enabled or self.enricher is None:
            return clusters
            
        logger.info(f"Enriching {len(clusters)} clusters with Yandex Direct data")
        
        enriched_clusters = []
        for cluster in clusters:
            try:
                enriched = self.enricher.enrich_cluster(cluster)
                enriched_clusters.append(enriched)
            except Exception as e:
                logger.error(f"Failed to enrich cluster: {e}")
                enriched_clusters.append(cluster)
                
        return enriched_clusters

