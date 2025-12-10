"""
Предварительная загрузка данных Yandex Direct в фоне.

Загружает данные для запросов, которых нет в кэше,
пока выполняются другие этапы анализа.
"""

from typing import List, Optional
import logging

from .yandex_direct_client import YandexDirectClient
from .yandex_direct_parser import YandexDirectParser
from ..core.yandex_direct_cache import YandexDirectCache


logger = logging.getLogger(__name__)


class YandexDirectPreloader:
    """Предварительная загрузка данных Direct в фоне."""
    
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
        
    def get_missing_queries(self, queries: List[str]) -> List[str]:
        """
        Получить список запросов, которых нет в кэше.
        
        Args:
            queries: Все запросы для проверки
            
        Returns:
            Список запросов без кэшированных данных
        """
        missing = []
        for query in queries:
            if not self.cache.get(query, self.client.geo_id):
                missing.append(query)
        return missing
        
    def preload_queries(self, queries: List[str], show_progress: bool = True) -> dict:
        """
        Предварительная загрузка данных для запросов.
        
        Args:
            queries: Список запросов для загрузки
            show_progress: Показывать прогресс загрузки
            
        Returns:
            Статистика: {'loaded': int, 'from_cache': int, 'failed': int, 'skipped_long': int}
        """
        stats = {'loaded': 0, 'from_cache': 0, 'failed': 0, 'skipped_long': 0}
        
        # Проверяем что есть в кэше
        missing = self.get_missing_queries(queries)
        stats['from_cache'] = len(queries) - len(missing)
        
        if not missing:
            return stats
        
        # Фильтруем запросы: только до 6 слов (ограничение API Direct)
        valid_queries = []
        for query in missing:
            if len(query.split()) <= 6:
                valid_queries.append(query)
            else:
                stats['skipped_long'] += 1
        
        if not valid_queries:
            return stats
        
        # Подготавливаем батчи
        batches = []
        for i in range(0, len(valid_queries), YandexDirectClient.MAX_PHRASES_PER_REQUEST):
            batch = valid_queries[i:i + YandexDirectClient.MAX_PHRASES_PER_REQUEST]
            batches.append(batch)
        
        total_batches = len(batches)
        
        # Оценка времени: конвейерная обработка
        estimated_time_sec = total_batches * 0.1  # ~0.1 сек на батч (конвейер)
        estimated_time_min = estimated_time_sec / 60
        
        if show_progress:
            print(f"  📥 Нужно загрузить: {len(valid_queries)} запросов ({total_batches} батчей)")
            if stats['skipped_long'] > 0:
                print(f"  ⚠️  Пропущено длинных (>6 слов): {stats['skipped_long']}")
            if estimated_time_min >= 1:
                print(f"  ⏱️  Примерное время: ~{estimated_time_min:.1f} мин")
            else:
                print(f"  ⏱️  Примерное время: ~{estimated_time_sec:.0f} сек")
        
        # Конвейерная обработка: всегда держим 5 активных прогнозов
        MAX_PARALLEL = 5
        active_forecasts = []  # [(forecast_id, batch, batch_num), ...]
        batch_idx = 0
        processed = 0
        
        # Запускаем первые 5 прогнозов
        while batch_idx < min(MAX_PARALLEL, total_batches):
            batch = batches[batch_idx]
            try:
                forecast_id = self.client.create_forecast(batch, debug=False, skip_wait=True)
                active_forecasts.append((forecast_id, batch, batch_idx + 1))
            except Exception as e:
                logger.error(f"Failed to create forecast for batch {batch_idx + 1}: {e}")
                stats['failed'] += len(batch)
            batch_idx += 1
        
        # Обрабатываем: получаем готовый, удаляем, запускаем следующий
        while active_forecasts:
            # Берём первый из активных (FIFO)
            forecast_id, batch, batch_num = active_forecasts.pop(0)
            
            try:
                # Получаем данные
                forecast_data = self.client.get_forecast(forecast_id, debug=False, skip_wait=True)
                
                # Парсим и сохраняем в БД
                parsed_data = self.parser.parse_forecast_response(forecast_data)
                for data in parsed_data:
                    self.cache.set(data, self.client.geo_id)
                    stats['loaded'] += 1
                
                # СРАЗУ удаляем из Яндекса
                self.client.delete_forecast(forecast_id, debug=False, skip_wait=True)
                
                processed += 1
                
                # СРАЗУ отправляем следующий батч (если есть)
                if batch_idx < total_batches:
                    next_batch = batches[batch_idx]
                    try:
                        next_forecast_id = self.client.create_forecast(next_batch, debug=False, skip_wait=True)
                        active_forecasts.append((next_forecast_id, next_batch, batch_idx + 1))
                    except Exception as e:
                        logger.error(f"Failed to create forecast for batch {batch_idx + 1}: {e}")
                        stats['failed'] += len(next_batch)
                    batch_idx += 1
                
            except Exception as e:
                logger.error(f"Failed to process batch {batch_num}: {e}")
                stats['failed'] += len(batch)
                # Пытаемся удалить даже при ошибке
                try:
                    self.client.delete_forecast(forecast_id, debug=False, skip_wait=True)
                except:
                    pass
                processed += 1
            
            # Показываем прогресс каждые 5 батчей
            if show_progress and processed % 5 == 0:
                progress_percent = (processed / total_batches) * 100
                queries_processed = min(processed * YandexDirectClient.MAX_PHRASES_PER_REQUEST, len(valid_queries))
                print(f"  ⏳ Прогресс: {processed}/{total_batches} батчей ({progress_percent:.1f}%) | {queries_processed}/{len(valid_queries)} запросов")
        
        return stats

