"""
Обработка батчей запросов в асинхронном режиме
"""

from typing import List, Dict, Any, Optional, Callable

from .master_db_handler import MasterDBHandler
from .result_formatter import ResultFormatter
from ..sync_batch_client import SyncBatchSERPClient
from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor
from ..utils.error_handler import create_error_result


class BatchProcessor:
    """Процессор батчей запросов"""
    
    def __init__(
        self,
        api_key: str,
        lr: int,
        master_db_handler: MasterDBHandler,
        result_formatter: ResultFormatter,
        stats: Dict[str, int],
        recovery_handler=None,
        device: str = 'desktop',
        site: str = None,
        proxies: List[str] = None,
        proxy_file: str = None
    ):
        """
        Args:
            api_key: API ключ
            lr: Регион поиска
            master_db_handler: Обработчик Master DB
            result_formatter: Форматтер результатов
            stats: Словарь со статистикой
            recovery_handler: Обработчик восстановления
            device: Устройство (desktop, mobile, tablet, iphone, android)
            site: Домен для фильтрации (site:domain.ru)
            proxies: Список прокси в формате ['http://user:pass@ip:port', ...]
            proxy_file: Путь к файлу с прокси (по одному на строку)
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.site = site
        self.master_db_handler = master_db_handler
        self.result_formatter = result_formatter
        self.stats = stats
        self.recovery_handler = recovery_handler
        self.proxies = proxies
        self.proxy_file = proxy_file
    
    async def analyze_queries_batch_async_mode(
        self,
        queries: List[str],
        progress_callback: Optional[Callable] = None,
        auto_recover: bool = True,
        query_to_group_map: Dict[str, str] = None
    ) -> List[Dict[str, Any]]:
        """
        Массовая асинхронная загрузка SERP (streaming mode)
        
        Args:
            queries: Список запросов
            progress_callback: Callback для прогресса
            auto_recover: Автоматически восстанавливать незавершённые запросы
            
        Returns:
            Список результатов SERP
        """
        # Автоматическое восстановление незавершённых запросов
        if auto_recover and self.recovery_handler:
            await self.recovery_handler.recover_pending_requests()
            print(f"\n⏭️  Продолжаем анализ текущей группы '{self.master_db_handler.query_group}'...")
        
        total = len(queries)
        self.stats['total_queries'] += total
        print(f"\n🚀 BATCH ASYNC MODE: {total} запросов")
        
        # Стриминг проверка кэша (батчами для ускорения)
        print(f"📦 Стриминг проверка кэша...")
        cached_results = {}
        uncached_queries = []
        empty_urls_count = 0  # Счетчик запросов с пустым serp_top_urls
        
        # УБРАНО ОГРАНИЧЕНИЕ: Проверяем кэш по ВСЕМ группам, а не только текущей
        # Это позволяет использовать кэш из других групп при объединенной обработке
        if self.master_db_handler.master_db:
            # Проверяем кэш батчами (по 1000 запросов) для ускорения
            cache_batch_size = 1000
            total_batches = (total + cache_batch_size - 1) // cache_batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * cache_batch_size
                end_idx = min(start_idx + cache_batch_size, total)
                batch_queries = queries[start_idx:end_idx]
                
                # Проверяем кэш для текущего батча (по всем группам)
                batch_results = self.master_db_handler.batch_get_from_master_db(batch_queries)
                
                # Обрабатываем результаты батча
                for query in batch_queries:
                    master_cached = batch_results.get(query)
                    if master_cached:
                        self.stats['cached_from_master'] += 1
                        cached_results[query] = master_cached
                    else:
                        # Данных нет или serp_top_urls пустой
                        empty_urls_count += 1
                        uncached_queries.append(query)
                
                # Показываем прогресс проверки кэша
                checked = end_idx
                cached_count = len(cached_results)
                uncached_count = len(uncached_queries)
                print(f"   Проверено: {checked}/{total} | Кэш: {cached_count} | Загрузить: {uncached_count}")
        else:
            # Нет Master DB - все запросы нужно загрузить
            uncached_queries = queries.copy()
        
        print(f"✓ Закэшировано: {len(cached_results)}/{total}")
        if empty_urls_count > 0:
            print(f"⚠️  Запросов с пустым serp_top_urls: {empty_urls_count} (будут загружены заново)")
        print(f"📤 Нужно загрузить: {len(uncached_queries)}")
        
        if not uncached_queries:
            return [cached_results.get(q, self._create_error_result(q, "Not found")) for q in queries]
        
        # Добавляем site: к запросам если указан домен
        actual_queries = []
        query_mapping = {}  # actual_query -> original_query
        for query in uncached_queries:
            if self.site:
                actual_query = f"{query} site:{self.site}"
            else:
                actual_query = query
            actual_queries.append(actual_query)
            query_mapping[actual_query] = query
        
        # Callback при получении req_id
        req_id_saved_count = [0]  # Используем список для изменения в замыкании
        data_saved_count = [0]  # Счетчик сохраненных данных
        
        def on_req_id_received(actual_query: str, req_id: str):
            """Сохраняем req_id в Master DB СРАЗУ после получения"""
            original_query = query_mapping.get(actual_query, actual_query)
            # Определяем group_name для запроса
            target_group = None
            if query_to_group_map:
                target_group = query_to_group_map.get(original_query)
            if not target_group:
                target_group = self.master_db_handler.query_group
            
            if self.master_db_handler.master_db and target_group:
                self.master_db_handler.update_master_status(
                    original_query, 'processing', req_id=req_id, group_name=target_group
                )
                req_id_saved_count[0] += 1
                # Логируем каждые 50 запросов для лучшей видимости прогресса
                if req_id_saved_count[0] % 50 == 0:
                    print(f"   📤 Отправлено запросов: {req_id_saved_count[0]}/{len(uncached_queries)}")
                elif req_id_saved_count[0] % 100 == 0:
                    print(f"   ✓ Сохранено req_id: {req_id_saved_count[0]} запросов")
        
        # Инициализируем обогатители один раз
        enricher = SERPDataEnricher()
        lsi_extractor = LSIExtractor()
        
        # Callback при получении результата - обрабатываем СРАЗУ
        def on_result_completed(raw_result: Dict[str, Any]):
            """Обработать и сохранить результат СРАЗУ после получения"""
            actual_query = raw_result.get('query', '')
            # Извлекаем оригинальный запрос из маппинга
            query = query_mapping.get(actual_query, actual_query)
            # Если запрос содержит site:, удаляем его для получения оригинального
            if self.site and query.endswith(f" site:{self.site}"):
                query = query[:-len(f" site:{self.site}")]
            
            if not query or query in cached_results:
                return
            
            req_id = raw_result.get('req_id')
            
            if raw_result.get('status') == 'completed':
                xml_text = raw_result.get('xml_response')
                if xml_text:
                    enriched = enricher.enrich_from_serp(xml_text, query)
                    lsi_phrases = lsi_extractor.extract_from_serp_documents(enriched['documents'], query)
                    
                    result = {
                        'query': query,
                        'lr': self.lr,
                        'source': 'api_batch_async',
                        'cached_at': None,
                        'error': None,
                        'status': 'completed',
                        'req_id': req_id,
                        'xml_response': xml_text,
                        'metrics': enriched['metrics'],
                        'documents': enriched['documents'],
                        'lsi_phrases': lsi_phrases
                    }
                    
                    # Сохраняем в Master DB СРАЗУ
                    # Определяем group_name для запроса
                    target_group = None
                    if query_to_group_map:
                        target_group = query_to_group_map.get(query)
                    if not target_group:
                        target_group = self.master_db_handler.query_group
                    
                    if self.master_db_handler.master_db and target_group:
                        self.master_db_handler.update_master_status(
                            query, 'completed', req_id=req_id, group_name=target_group
                        )
                        self.master_db_handler.master_db.update_serp_metrics(
                            group_name=target_group,
                            keyword=query,
                            metrics=enriched['metrics'],
                            documents=enriched['documents'],
                            lsi_phrases=lsi_phrases
                        )
                        
                        # Увеличиваем счетчик сохраненных данных
                        data_saved_count[0] += 1
                        
                        # Логирование сохранения (каждые 100 запросов для читаемости)
                        if data_saved_count[0] % 100 == 0:
                            urls_count = len(enriched['documents'])
                            lsi_count = len(lsi_phrases)
                            print(f"   💾 Сохранено данных в БД: {data_saved_count[0]} запросов (последний: {urls_count} URLs, {lsi_count} LSI фраз)")
                        else:
                            # Для остальных запросов - только краткое логирование каждые 10
                            if data_saved_count[0] % 10 == 0:
                                query_short = query[:50] + "..." if len(query) > 50 else query
                                urls_count = len(enriched['documents'])
                                lsi_count = len(lsi_phrases)
                                print(f"     ✓ '{query_short}': {urls_count} URLs, {lsi_count} LSI фраз")
                    
                    cached_results[query] = result
                    self.stats['api_requests'] += 1
                else:
                    # Нет XML ответа
                    result = {
                        'query': query,
                        'lr': self.lr,
                        'source': 'error',
                        'cached_at': None,
                        'error': 'No XML response',
                        'status': 'error',
                        'req_id': req_id,
                        'metrics': SERPDataEnricher()._get_empty_metrics(),
                        'documents': [],
                        'lsi_phrases': []
                    }
                    cached_results[query] = result
                    self.stats['errors'] += 1
            else:
                # Ошибка или другой статус
                error_msg = raw_result.get('error', 'Unknown error')
                
                result = {
                    'query': query,
                    'lr': self.lr,
                    'source': 'error',
                    'cached_at': None,
                    'error': error_msg,
                    'status': raw_result.get('status', 'error'),
                    'req_id': req_id,
                    'metrics': SERPDataEnricher()._get_empty_metrics(),
                    'documents': [],
                    'lsi_phrases': []
                }
                
                if self.master_db_handler.master_db and self.master_db_handler.query_group:
                    self.master_db_handler.update_master_status(
                        query, 'error', req_id=req_id, error_message=error_msg
                    )
                
                cached_results[query] = result
                self.stats['errors'] += 1
        
        # НОВАЯ ЛОГИКА: Общая очередь запросов, все прокси работают асинхронно
        # Каждый прокси берет запросы из общей очереди и обрабатывает полностью: отправил → получил → пошел дальше
        from ..batch.proxy_manager import ProxyManager
        import asyncio
        
        temp_proxy_manager = ProxyManager(proxies=self.proxies, proxy_file=self.proxy_file)
        proxy_count = temp_proxy_manager.get_proxy_count()
        
        if proxy_count > 0:
            # Используем новый асинхронный клиент с общей очередью
            from ..async_queue_client import AsyncQueueSERPClient
            
            client = AsyncQueueSERPClient(
                api_key=self.api_key,
                lr=self.lr,
                requests_per_second=40.0,  # 40 запросов в секунду на IP (увеличено)
                initial_delay=0.5,  # Проверка через 0.5 сек после отправки (уменьшено)
                retry_delay=0.5,  # 0.5 сек между попытками (уменьшено)
                max_attempts=50,
                device=self.device,
                proxies=self.proxies,
                proxy_file=self.proxy_file,
                silent=False
            )
            
            try:
                batch_result = await client.process_queries_batch(
                    queries=actual_queries,
                    progress_callback=progress_callback,
                    on_req_id_received=on_req_id_received,
                    on_result_completed=on_result_completed
                )
            finally:
                await client.close()
        else:
            # Нет прокси - используем один клиент со строго последовательной обработкой
            print(f"⚡ Без прокси - строго последовательная обработка")
            print(f"   Обработка: отправил → через {2}с проверил → записал → следующий запрос")
            print(f"   Rate limit: максимум 50 запросов в секунду")
            print(f"   Строго последовательно: один запрос за раз")
            
            from ..sync_batch_client import SyncBatchSERPClient
            
            client = SyncBatchSERPClient(
                api_key=self.api_key,
                lr=self.lr,
                max_concurrent_send=1,  # Строго последовательно: один запрос за раз
                max_concurrent_fetch=1,  # То же самое
                initial_delay=2,
                retry_delay=2,
                max_attempts=50,
                requests_per_second=50.0,  # Лимит 50 запросов в секунду
                device=self.device,
                proxies=None,
                proxy_file=None,
                silent=False
            )
            
            try:
                batch_result = await client.process_queries_batch(
                    queries=actual_queries,
                    progress_callback=progress_callback,
                    on_req_id_received=on_req_id_received,
                    on_result_completed=on_result_completed
                )
            finally:
                await client.close()
        
        # batch_result уже получен выше (в блоке с прокси или без прокси)
        # Результаты УЖЕ обработаны через callback on_result_completed
        # Обрабатываем только ошибки которые не попали в callback
        if 'batch_result' in locals():
            for raw_result in batch_result.get('results', []):
                query = raw_result.get('query')
                if query and query not in cached_results:
                    # Обрабатываем только те, что не были обработаны в callback
                    if raw_result.get('status') != 'completed':
                        error_msg = raw_result.get('error', 'Unknown error')
                        result = {
                            'query': query,
                            'lr': self.lr,
                            'source': 'error',
                            'cached_at': None,
                            'error': error_msg,
                            'status': raw_result.get('status', 'error'),
                            'req_id': raw_result.get('req_id'),
                            'metrics': SERPDataEnricher()._get_empty_metrics(),
                            'documents': [],
                            'lsi_phrases': []
                        }
                        cached_results[query] = result
                        self.stats['errors'] += 1
        
        # Возвращаем в исходном порядке
        return [cached_results.get(q, self._create_error_result(q, "Not processed")) for q in queries]
    
    def _create_error_result(self, query: str, error: str) -> Dict[str, Any]:
        """Создать результат с ошибкой"""
        empty_metrics = SERPDataEnricher()._get_empty_metrics()
        return create_error_result(query, error, self.lr, empty_metrics)

