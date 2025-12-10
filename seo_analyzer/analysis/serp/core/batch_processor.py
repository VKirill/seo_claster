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
        site: str = None
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
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.site = site
        self.master_db_handler = master_db_handler
        self.result_formatter = result_formatter
        self.stats = stats
        self.recovery_handler = recovery_handler
    
    async def analyze_queries_batch_async_mode(
        self,
        queries: List[str],
        progress_callback: Optional[Callable] = None,
        auto_recover: bool = True
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
        
        # Проверка кэша
        print(f"📦 Проверка кэша...")
        cached_results = {}
        uncached_queries = []
        
        for query in queries:
            if self.master_db_handler.master_db and self.master_db_handler.query_group:
                master_cached = self.master_db_handler.get_from_master_db(query)
                if master_cached:
                    self.stats['cached_from_master'] += 1
                    cached_results[query] = master_cached
                    continue
            uncached_queries.append(query)
        
        print(f"✓ Закэшировано: {len(cached_results)}/{total}")
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
        
        # Streaming обработка через SyncBatchSERPClient
        # Используем максимальные значения из рекомендаций сервиса:
        # - Не более 50 одновременных потоков
        # - Не более 100 запросов в секунду
        batch_client = SyncBatchSERPClient(
            api_key=self.api_key,
            lr=self.lr,
            max_concurrent_send=50,  # Максимум из рекомендаций
            max_concurrent_fetch=50,  # Максимум из рекомендаций
            initial_delay=10,
            retry_delay=10,
            max_attempts=100,
            requests_per_second=100.0,  # Максимум из рекомендаций
            device=self.device
        )
        
        # Callback при получении req_id
        req_id_saved_count = [0]  # Используем список для изменения в замыкании
        
        def on_req_id_received(actual_query: str, req_id: str):
            """Сохраняем req_id в Master DB СРАЗУ после получения"""
            original_query = query_mapping.get(actual_query, actual_query)
            if self.master_db_handler.master_db and self.master_db_handler.query_group:
                self.master_db_handler.update_master_status(original_query, 'processing', req_id=req_id)
                req_id_saved_count[0] += 1
                if req_id_saved_count[0] % 100 == 0:
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
                    if self.master_db_handler.master_db and self.master_db_handler.query_group:
                        self.master_db_handler.update_master_status(query, 'completed', req_id=req_id)
                        self.master_db_handler.master_db.update_serp_metrics(
                            group_name=self.master_db_handler.query_group,
                            keyword=query,
                            metrics=enriched['metrics'],
                            documents=enriched['documents'],
                            lsi_phrases=lsi_phrases
                        )
                        
                        # Логирование сохранения
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
        
        # Запускаем streaming обработку
        try:
            batch_result = await batch_client.process_queries_batch(
                queries=actual_queries,
                progress_callback=progress_callback,
                on_req_id_received=on_req_id_received,
                on_result_completed=on_result_completed,
                batch_size=50,
                completion_threshold=0.95
            )
            
            # Результаты УЖЕ обработаны через callback on_result_completed
            # Обрабатываем только ошибки которые не попали в callback
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
                query = raw_result.get('query')
                if not query or query in cached_results:
                    continue
                
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
                        
                        # Сохраняем в Master DB
                        if self.master_db_handler.master_db and self.master_db_handler.query_group:
                            self.master_db_handler.update_master_status(query, 'completed', req_id=req_id)
                            self.master_db_handler.master_db.update_serp_metrics(
                                group_name=self.master_db_handler.query_group,
                                keyword=query,
                                metrics=enriched['metrics'],
                                documents=enriched['documents'],
                                lsi_phrases=lsi_phrases
                            )
                            
                            # Логирование сохранения
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
        finally:
            await batch_client.close()
        
        # Возвращаем в исходном порядке
        return [cached_results.get(q, self._create_error_result(q, "Not processed")) for q in queries]
    
    def _create_error_result(self, query: str, error: str) -> Dict[str, Any]:
        """Создать результат с ошибкой"""
        empty_metrics = SERPDataEnricher()._get_empty_metrics()
        return create_error_result(query, error, self.lr, empty_metrics)

