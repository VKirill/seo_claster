"""
Анализ отдельных запросов SERP
"""

import asyncio
from typing import List, Dict, Any, Optional, Callable

from .master_db_handler import MasterDBHandler
from .result_formatter import ResultFormatter
from ..utils.error_handler import create_error_result


class QueryAnalyzer:
    """Анализатор отдельных запросов"""
    
    def __init__(
        self,
        api_client,
        master_db_handler: MasterDBHandler,
        result_formatter: ResultFormatter,
        stats: Dict[str, int],
        lr: int
    ):
        """
        Args:
            api_client: API клиент для запросов
            master_db_handler: Обработчик Master DB
            result_formatter: Форматтер результатов
            stats: Словарь со статистикой
            lr: Регион поиска
        """
        self.api_client = api_client
        self.master_db_handler = master_db_handler
        self.result_formatter = result_formatter
        self.stats = stats
        self.lr = lr
    
    async def analyze_query(
        self,
        query: str,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Анализировать один запрос с отслеживанием статуса
        
        Args:
            query: Запрос
            force_refresh: Принудительно обновить из API
            
        Returns:
            Результат анализа
        """
        self.stats['total_queries'] += 1
        
        # Проверяем Master DB (единственный источник)
        if not force_refresh:
            if self.master_db_handler.master_db and self.master_db_handler.query_group:
                master_cached = self.master_db_handler.get_from_master_db(query)
                if master_cached:
                    self.stats['cached_from_master'] += 1
                    return master_cached
        
        # Нет в Master DB - делаем запрос к API
        return await self._fetch_from_api(query)
    
    async def analyze_queries_batch(
        self,
        queries: List[str],
        max_concurrent: int = None,
        progress_callback: Optional[Callable] = None,
        batch_size: int = 500,
        use_batch_async: bool = True,
        batch_processor=None,
        query_to_group_map: Dict[str, str] = None
    ) -> List[Dict[str, Any]]:
        """
        Анализировать пакет запросов с батчингом
        
        Args:
            queries: Список запросов
            max_concurrent: Игнорируется (для обратной совместимости)
            progress_callback: Callback для прогресса
            batch_size: Размер батча (по умолчанию 500)
            use_batch_async: Использовать асинхронный режим
            batch_processor: Процессор батчей (если use_batch_async=True)
            
        Returns:
            Список результатов SERP анализа
        """
        if not queries:
            return []
        
        # Если включен batch async режим - используем его!
        if use_batch_async and batch_processor:
            return await batch_processor.analyze_queries_batch_async_mode(
                queries, progress_callback, query_to_group_map=query_to_group_map
            )
        
        # Иначе используем старый режим (батчами)
        total_queries = len(queries)
        self.stats['total_queries'] += total_queries
        
        # ОПТИМИЗАЦИЯ: Массовая загрузка из Master DB
        cached_data = {}
        queries_need_api = []
        
        for query in queries:
            if self.master_db_handler.master_db and self.master_db_handler.query_group:
                master_cached = self.master_db_handler.get_from_master_db(query)
                if master_cached:
                    self.stats['cached_from_master'] += 1
                    cached_data[query] = master_cached
                    continue
            queries_need_api.append(query)
        
        # Разделяем на закэшированные и требующие API
        cached_queries = list(cached_data.keys())
        
        all_results = []
        
        # Добавляем закэшированные результаты (мгновенно)
        for query in cached_queries:
            all_results.append(cached_data[query])
            if progress_callback:
                progress_callback(len(all_results), total_queries, query)
        
        # Обрабатываем запросы требующие API (streaming режим)
        if queries_need_api:
            async def fetch_with_progress(query: str, index: int):
                """Обработать один запрос: отправить → получить → обработать → сохранить"""
                result = await self._fetch_from_api(query)
                if progress_callback:
                    progress_callback(len(cached_queries) + index + 1, total_queries, query)
                return result
            
            # Обрабатываем API запросы параллельно (streaming режим)
            # Каждый запрос обрабатывается независимо: отправил → получил → обработал → следующий
            tasks = [
                fetch_with_progress(q, i) 
                for i, q in enumerate(queries_need_api)
            ]
            
            # Запускаем все задачи параллельно
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обработка исключений
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    self.stats['errors'] += 1
                    empty_metrics = self.api_client.enricher._get_empty_metrics()
                    all_results.append(
                        create_error_result(queries_need_api[i], str(result), self.lr, empty_metrics)
                    )
                else:
                    all_results.append(result)
        
        return all_results
    
    async def _fetch_from_api(self, query: str) -> Dict[str, Any]:
        """
        Запрос к API через клиент с отслеживанием статуса
        
        Args:
            query: Запрос
            
        Returns:
            Результат запроса
        """
        self.stats['api_requests'] += 1
        
        # Отмечаем как processing в Master DB
        if self.master_db_handler.master_db and self.master_db_handler.query_group:
            self.master_db_handler.update_master_status(query, 'processing')
        
        api_result = await self.api_client.fetch_serp_data(query)
        
        # Извлекаем req_id если есть
        req_id = api_result.get('req_id')
        
        if api_result.get('error'):
            self.stats['errors'] += 1
            error_text = api_result['error']
            
            # Проверяем тип ошибки
            is_temporary_error = any(keyword in error_text.lower() for keyword in [
                'timeout',           # Timeout ошибки
                'code="202"',        # Запрос не обработан
                'code="210"',        # Запрос в очереди
                'не обработан',      # Русский текст ошибки 202
                'в очередь',         # Русский текст ошибки 210
                'network error',     # Сетевые ошибки
            ])
            
            result = self.result_formatter.create_result(query, api_result, 'error', error_text)
            
            # Добавляем req_id и status в результат
            result['req_id'] = req_id
            result['status'] = 'processing' if is_temporary_error else 'error'
            
            # Обновляем статус в Master DB
            if self.master_db_handler.master_db and self.master_db_handler.query_group:
                if is_temporary_error:
                    # Временная ошибка - оставляем processing
                    self.master_db_handler.update_master_status(
                        query, 'processing', req_id=req_id, error_message=error_text
                    )
                else:
                    # Постоянная ошибка
                    self.master_db_handler.update_master_status(
                        query, 'error', req_id=req_id, error_message=error_text
                    )
            
            return result
        
        result = self.result_formatter.create_result(query, api_result)
        
        # Добавляем req_id и status в результат
        result['req_id'] = req_id
        result['status'] = 'completed'
        
        # Отмечаем как completed в Master DB + сохраняем данные
        if self.master_db_handler.master_db and self.master_db_handler.query_group:
            self.master_db_handler.update_master_status(query, 'completed', req_id=req_id)
        
        return result

