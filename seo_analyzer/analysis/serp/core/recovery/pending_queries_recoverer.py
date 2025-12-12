"""
Восстановление незавершённых запросов по req_id
"""

import asyncio
from typing import List, Dict, Any, Optional

from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor


class PendingQueriesRecoverer:
    """Восстановитель незавершённых запросов"""
    
    def __init__(
        self, 
        api_key: str, 
        lr: int, 
        master_db_handler,
        proxies: Optional[List[str]] = None,
        proxy_file: Optional[str] = None
    ):
        """
        Args:
            api_key: API ключ
            lr: Регион поиска
            master_db_handler: Обработчик Master DB
            proxies: Список прокси в формате ['http://user:pass@ip:port', ...]
            proxy_file: Путь к файлу с прокси (по одному на строку)
        """
        self.api_key = api_key
        self.lr = lr
        self.master_db_handler = master_db_handler
        self.proxies = proxies
        self.proxy_file = proxy_file
    
    async def recover(self, pending_queries: List[Dict[str, Any]]) -> int:
        """
        Восстановить незавершённые запросы с использованием прокси для параллельной обработки
        
        Args:
            pending_queries: Список незавершённых запросов
            
        Returns:
            Количество восстановленных запросов
        """
        if not pending_queries:
            return 0
        
        recoverable_by_req_id = [
            item for item in pending_queries 
            if item.get('req_id') and not item.get('needs_new_request', False)
        ]
        
        if not recoverable_by_req_id:
            return 0
        
        # Создаем словарь для быстрого доступа к группе
        group_map = {}
        for item in recoverable_by_req_id:
            group_map[item['query']] = item['group']
        
        # Подготавливаем запросы для восстановления (req_id уже есть)
        queries_to_recover = [
            {
                'query': item['query'],
                'req_id': item['req_id'],
                'group': item['group']
            }
            for item in recoverable_by_req_id
        ]
        
        # Загружаем прокси и создаем общую очередь запросов
        from ...batch.proxy_manager import ProxyManager
        from ...batch.rate_limiter import RateLimiter
        proxy_manager = ProxyManager(proxies=self.proxies, proxy_file=self.proxy_file, silent=True)
        proxy_count = proxy_manager.get_proxy_count()
        requests_per_second = 30.0  # Rate limit: 30 запросов в секунду на IP
        
        enricher = SERPDataEnricher()
        lsi_extractor = LSIExtractor()
        recovered_count = [0]  # Используем список для изменения в замыкании
        
        if proxy_count > 0:
            # НОВАЯ ЛОГИКА: Общая очередь запросов, все прокси работают параллельно
            print(f"⚡ НОВАЯ ЛОГИКА: {proxy_count} прокси работают асинхронно")
            print(f"   Общая очередь запросов: {len(queries_to_recover)} запросов")
            print(f"   Rate limit: {requests_per_second} запросов в секунду на IP")
            print(f"   Логика: получили → обработали → пошли дальше")
            print(f"   Все прокси берут запросы из общей очереди параллельно")
            
            # Общая очередь запросов
            queue = asyncio.Queue()
            for query_data in queries_to_recover:
                await queue.put(query_data)
            
            # Rate limiters для каждого прокси
            rate_limiters = {}
            for proxy_url in proxy_manager.proxies:
                rate_limiters[proxy_url] = RateLimiter(requests_per_second)
            
            # Lock для статистики
            stats_lock = asyncio.Lock()
            
            async def process_proxy_recovery(proxy_url: str):
                """Обработать запросы через конкретный прокси из общей очереди"""
                proxy_short = proxy_url.split('@')[1] if '@' in proxy_url else proxy_url[:30]
                proxy_recovered = [0]  # Счетчик восстановленных для этого прокси
                
                # Фиксированный прокси для всех запросов этого потока
                proxies_dict = {'http': proxy_url, 'https': proxy_url}
                rate_limiter = rate_limiters[proxy_url]
                
                # Парсим API ключ
                if ':' in self.api_key:
                    api_user, api_key_val = self.api_key.split(':', 1)
                else:
                    api_user = api_key_val = self.api_key
                
                import requests
                from requests.exceptions import ProxyError, ConnectTimeout, ConnectionError as RequestsConnectionError
                import re
                
                while True:
                    try:
                        # Берем запрос из очереди (с таймаутом)
                        try:
                            query_data = await asyncio.wait_for(queue.get(), timeout=0.1)
                        except asyncio.TimeoutError:
                            # Очередь пуста, проверяем еще раз
                            if queue.empty():
                                break
                            continue
                        
                        query = query_data['query']
                        req_id = query_data['req_id']
                        
                        # Rate limit для этого прокси
                        await rate_limiter.wait_for_rate_limit()
                        await RateLimiter.check_and_wait_for_503()
                        
                        # Опрашиваем до получения результата
                        attempt = 0
                        consecutive_errors = 0
                        consecutive_proxy_errors = 0
                        max_consecutive_errors = 10
                        max_consecutive_proxy_errors = 50
                        
                        while attempt < 200:  # Максимум попыток
                            attempt += 1
                            
                            def fetch_result():
                                params = {'user': api_user, 'key': api_key_val, 'req_id': req_id}
                                try:
                                    response = requests.get(
                                        "https://xmlstock.com/yandex/xml/", 
                                        params=params, 
                                        timeout=(30, 10),
                                        proxies=proxies_dict
                                    )
                                    if response.status_code == 200:
                                        xml_text = response.text
                                        if '<error' in xml_text:
                                            error_match = re.search(r'<error[^>]*code="([^"]*)"', xml_text)
                                            if error_match and error_match.group(1) == '202':
                                                return {'status': 'pending'}
                                        return {
                                            'query': query,
                                            'req_id': req_id,
                                            'status': 'completed',
                                            'xml_response': xml_text
                                        }
                                    
                                    # Проверка на 503
                                    is_503 = response.status_code == 503
                                    return {
                                        'query': query,
                                        'status': 'error',
                                        'error': f"HTTP {response.status_code}",
                                        'is_503': is_503
                                    }
                                except (ProxyError, ConnectTimeout, RequestsConnectionError) as e:
                                    return {
                                        'query': query, 
                                        'status': 'proxy_error', 
                                        'error': str(e)
                                    }
                                except Exception as e:
                                    return {'query': query, 'status': 'error', 'error': str(e)}
                            
                            result = await asyncio.get_event_loop().run_in_executor(None, fetch_result)
                            
                            # Обработка 503 ошибки
                            if result.get('is_503'):
                                await RateLimiter.mark_503_error()
                            
                            if result.get('status') == 'completed':
                                # Обрабатываем результат
                                xml_text = result.get('xml_response')
                                if xml_text:
                                    enriched = enricher.enrich_from_serp(xml_text, query)
                                    lsi_phrases = lsi_extractor.extract_from_serp_documents(
                                        enriched['documents'], query
                                    )
                                    
                                    original_group = group_map.get(query)
                                    if self.master_db_handler.master_db and original_group:
                                        try:
                                            self.master_db_handler.master_db.update_serp_status(
                                                group_name=original_group,
                                                keyword=query,
                                                status='completed',
                                                req_id=req_id
                                            )
                                            self.master_db_handler.master_db.update_serp_metrics(
                                                group_name=original_group,
                                                keyword=query,
                                                metrics=enriched['metrics'],
                                                documents=enriched['documents'],
                                                lsi_phrases=lsi_phrases
                                            )
                                            
                                            query_short = query[:50] + "..." if len(query) > 50 else query
                                            urls_count = len(enriched['documents'])
                                            lsi_count = len(lsi_phrases)
                                            
                                            async with stats_lock:
                                                recovered_count[0] += 1
                                                proxy_recovered[0] += 1
                                            
                                            # Логируем каждые 10 запросов
                                            if proxy_recovered[0] % 10 == 0:
                                                print(f"   [{proxy_short}] Прогресс: {proxy_recovered[0]} восстановлено")
                                            elif proxy_recovered[0] % 5 == 0:
                                                print(f"     ✓ '{query_short}': {urls_count} URLs, {lsi_count} LSI фраз")
                                        except Exception as e:
                                            print(f"   ⚠️  Ошибка сохранения для '{query[:50]}...': {e}")
                                
                                queue.task_done()
                                break
                            
                            elif result.get('status') == 'pending':
                                consecutive_errors = 0
                                consecutive_proxy_errors = 0
                                await asyncio.sleep(1.0)  # Уменьшили задержку до 1 сек
                                if attempt % 50 == 0:
                                    print(f"   [{proxy_short}] ⏳ '{query[:50]}...' еще обрабатывается (попытка {attempt})")
                                continue
                            
                            elif result.get('status') == 'proxy_error':
                                consecutive_proxy_errors += 1
                                consecutive_errors = 0
                                
                                if consecutive_proxy_errors >= max_consecutive_proxy_errors:
                                    query_short = query[:50] + "..." if len(query) > 50 else query
                                    print(f"   ❌ '{query_short}' ошибка прокси после {consecutive_proxy_errors} попыток")
                                    queue.task_done()
                                    break
                                
                                await asyncio.sleep(2.0)
                                continue
                            
                            else:
                                consecutive_errors += 1
                                consecutive_proxy_errors = 0
                                if consecutive_errors >= max_consecutive_errors:
                                    error_msg = result.get('error', 'Unknown error')
                                    query_short = query[:50] + "..." if len(query) > 50 else query
                                    print(f"   ❌ '{query_short}' ошибка после {consecutive_errors} попыток: {error_msg[:100]}")
                                    queue.task_done()
                                    break
                                
                                await asyncio.sleep(1.0)
                                continue
                        
                        # Достигнут лимит попыток
                        if attempt >= 200:
                            query_short = query[:50] + "..." if len(query) > 50 else query
                            print(f"   ❌ '{query_short}' превышен лимит попыток ({attempt})")
                            queue.task_done()
                    
                    except Exception as e:
                        print(f"   ⚠️  Ошибка обработки запроса через прокси {proxy_url[:30]}...: {e}")
                        queue.task_done()
                        continue
                
                print(f"   ✅ [{proxy_short}] Завершено: {proxy_recovered[0]} запросов восстановлено")
            
            # Запускаем обработку через все прокси параллельно
            proxy_tasks = [
                asyncio.create_task(process_proxy_recovery(proxy_url))
                for proxy_url in proxy_manager.proxies
            ]
            
            # Ждем завершения всех задач
            await asyncio.gather(*proxy_tasks, return_exceptions=True)
            
            # Ждем завершения всех задач из очереди
            await queue.join()
        else:
            # Нет прокси - используем прямой подход без прокси
            max_concurrent_no_proxy = 30  # Максимум параллельных запросов без прокси
            print(f"   ⚠️  Прокси не найдены - используем {max_concurrent_no_proxy} потоков без прокси")
            
            # Парсим API ключ (используем локальные переменные)
            if ':' in self.api_key:
                api_user, api_key_val = self.api_key.split(':', 1)
            else:
                api_user = api_key_val = self.api_key
            
            import requests
            from requests.exceptions import ConnectTimeout, ConnectionError as RequestsConnectionError
            import re
            
            semaphore = asyncio.Semaphore(max_concurrent_no_proxy)
            
            async def fetch_by_req_id_no_proxy(query_data: Dict[str, Any]):
                """Получить результат по req_id без прокси"""
                async with semaphore:
                    query = query_data['query']
                    req_id = query_data['req_id']
                    
                    attempt = 0
                    consecutive_errors = 0
                    max_consecutive_errors = 10
                    
                    while True:
                        attempt += 1
                        
                        def fetch_result():
                            params = {'user': api_user, 'key': api_key_val, 'req_id': req_id}
                            try:
                                response = requests.get("https://xmlstock.com/yandex/xml/", params=params, timeout=(30, 10))
                                if response.status_code == 200:
                                    xml_text = response.text
                                    if '<error' in xml_text:
                                        error_match = re.search(r'<error[^>]*code="([^"]*)"', xml_text)
                                        if error_match and error_match.group(1) == '202':
                                            return {'status': 'pending'}
                                    return {
                                        'query': query,
                                        'req_id': req_id,
                                        'status': 'completed',
                                        'xml_response': xml_text
                                    }
                                return {'query': query, 'status': 'error', 'error': f"HTTP {response.status_code}"}
                            except Exception as e:
                                return {'query': query, 'status': 'error', 'error': str(e)}
                        
                        result = await asyncio.get_event_loop().run_in_executor(None, fetch_result)
                        
                        if result.get('status') == 'completed':
                            xml_text = result.get('xml_response')
                            if xml_text:
                                enriched = enricher.enrich_from_serp(xml_text, query)
                                lsi_phrases = lsi_extractor.extract_from_serp_documents(
                                    enriched['documents'], query
                                )
                                
                                original_group = group_map.get(query)
                                if self.master_db_handler.master_db and original_group:
                                    try:
                                        self.master_db_handler.master_db.update_serp_status(
                                            group_name=original_group,
                                            keyword=query,
                                            status='completed',
                                            req_id=req_id
                                        )
                                        self.master_db_handler.master_db.update_serp_metrics(
                                            group_name=original_group,
                                            keyword=query,
                                            metrics=enriched['metrics'],
                                            documents=enriched['documents'],
                                            lsi_phrases=lsi_phrases
                                        )
                                        
                                        query_short = query[:50] + "..." if len(query) > 50 else query
                                        urls_count = len(enriched['documents'])
                                        lsi_count = len(lsi_phrases)
                                        print(f"     ✓ '{query_short}': {urls_count} URLs, {lsi_count} LSI фраз")
                                        
                                        recovered_count[0] += 1
                                    except Exception as e:
                                        print(f"   ⚠️  Ошибка сохранения для '{query[:50]}...': {e}")
                            return
                        elif result.get('status') == 'pending':
                            consecutive_errors = 0
                            await asyncio.sleep(10)
                            if attempt % 50 == 0:
                                print(f"   ⏳ '{query[:50]}...' еще обрабатывается (попытка {attempt})")
                            continue
                        else:
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                error_msg = result.get('error', 'Unknown error')
                                query_short = query[:50] + "..." if len(query) > 50 else query
                                print(f"   ❌ '{query_short}' ошибка после {consecutive_errors} попыток: {error_msg[:100]}")
                                return
                            await asyncio.sleep(10)
                            continue
            
            tasks = [fetch_by_req_id_no_proxy(q) for q in queries_to_recover]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        return recovered_count[0]

