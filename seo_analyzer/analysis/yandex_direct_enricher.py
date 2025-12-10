"""
Обогащение кластеров данными из Yandex Direct.

Добавляет реальные данные о трафике, CPC и конкуренции к результатам кластеризации.
"""

from typing import List, Dict, Optional
import logging
import os

from .yandex_direct_client import YandexDirectClient
from .yandex_direct_parser import YandexDirectParser
from .yandex_direct_aggregator import aggregate_cluster_metrics, get_empty_metrics
from ..core.yandex_direct_cache import YandexDirectCache


logger = logging.getLogger(__name__)


class YandexDirectEnricher:
    """Обогащение данных кластеров информацией из Yandex Direct."""
    
    def __init__(self, token: str, use_sandbox: bool = False, geo_id: int = 213,
                 minus_words_file: str = "yandex_direct_minus_words.txt",
                 db_path: Optional[str] = None):
        """
        Инициализация.
        
        Args:
            token: OAuth токен Yandex Direct
            use_sandbox: Использовать sandbox API
            geo_id: ID региона (213 = Москва)
            minus_words_file: Путь к файлу с минус-словами
            db_path: Путь к БД (по умолчанию output/serp_data.db)
        """
        self.client = YandexDirectClient(token, use_sandbox, geo_id, minus_words_file)
        self.parser = YandexDirectParser()
        
        # Если путь не указан, используем output/serp_data.db
        if db_path is None:
            db_path = "output/serp_data.db"
        
        self.cache = YandexDirectCache(db_path=db_path)
        
    def enrich_queries(self, queries: List[str], use_cache: bool = True) -> Dict[str, Dict]:
        """
        Обогащение списка запросов данными Direct.
        
        Args:
            queries: Список ключевых запросов
            use_cache: Использовать кэш
            
        Returns:
            Dict[query -> direct_data]
        """
        results = {}
        queries_to_fetch = []
        
        # Проверка кэша
        for query in queries:
            if use_cache:
                cached = self.cache.get(query, self.client.geo_id)
                if cached:
                    results[query] = cached
                    continue
                    
            queries_to_fetch.append(query)
            
        if not queries_to_fetch:
            return results
            
        # Проверяем debug режим из переменной окружения
        debug_mode = os.environ.get('YANDEX_DIRECT_DEBUG', '').lower() in ('1', 'true', 'yes')
        
        # Очистка старых прогнозов перед началом сбора
        if debug_mode:
            print("🗑️  Проверка и очистка старых прогнозов...")
        deleted = self.client.cleanup_old_forecasts(debug=debug_mode)
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old forecasts")
            if debug_mode:
                print(f"✓ Удалено старых прогнозов: {deleted}")
        elif debug_mode:
            print("✓ Старых прогнозов нет")
        
        # Запрос данных батчами (по 10 фраз)
        for i in range(0, len(queries_to_fetch), YandexDirectClient.MAX_PHRASES_PER_REQUEST):
            batch = queries_to_fetch[i:i + YandexDirectClient.MAX_PHRASES_PER_REQUEST]
            
            forecast_id = None
            try:
                # Создание и получение прогноза
                forecast_id = self.client.create_forecast(batch, debug=debug_mode)
                forecast_data = self.client.get_forecast(forecast_id, debug=debug_mode)
                
                # Парсинг ответа
                parsed_data = self.parser.parse_forecast_response(forecast_data)
                
                # Сохранение результатов
                for data in parsed_data:
                    phrase = data["phrase"]
                    results[phrase] = data
                    
                    if use_cache:
                        self.cache.set(data, self.client.geo_id)
                
                # Удаляем прогноз после получения данных (экономия ресурсов API)
                if forecast_id:
                    self.client.delete_forecast(forecast_id, debug=debug_mode)
                        
            except Exception as e:
                logger.error(f"Failed to fetch Direct data for batch: {e}")
                # Заполняем пустыми данными
                for query in batch:
                    results[query] = get_empty_metrics(query)
                
                # Пытаемся удалить прогноз даже в случае ошибки
                if forecast_id:
                    try:
                        self.client.delete_forecast(forecast_id, debug=False)
                    except:
                        pass  # Игнорируем ошибки удаления
                    
        return results
        
    def enrich_cluster(self, cluster: Dict) -> Dict:
        """
        Обогащение одного кластера.
        
        Args:
            cluster: Данные кластера с полем 'queries'
            
        Returns:
            Кластер с добавленными полями direct_*
        """
        queries = cluster.get("queries", [])
        if not queries:
            return cluster
            
        # Получаем данные для всех запросов кластера
        direct_data = self.enrich_queries(queries)
        
        # Агрегируем метрики кластера
        cluster_metrics = aggregate_cluster_metrics(direct_data)
        
        # Добавляем данные в кластер
        cluster["direct_shows"] = cluster_metrics["total_shows"]
        cluster["direct_clicks"] = cluster_metrics["total_clicks"]
        cluster["direct_avg_cpc"] = cluster_metrics["avg_cpc"]
        cluster["direct_competition"] = cluster_metrics["competition_level"]
        cluster["direct_recommended_cpc"] = cluster_metrics["recommended_cpc"]
        
        return cluster

