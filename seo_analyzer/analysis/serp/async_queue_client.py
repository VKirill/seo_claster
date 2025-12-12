"""
Асинхронный клиент с общей очередью запросов
Все прокси работают параллельно, берут запросы из общей очереди
Rate limit: 30 запросов в секунду на IP
"""

import asyncio
import requests
import re
import time
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

from .batch.rate_limiter import RateLimiter
from .sync_batch.executor_manager import ExecutorManager
from .batch.proxy_manager import ProxyManager
from requests.exceptions import ProxyError, ConnectTimeout, ConnectionError as RequestsConnectionError


@dataclass
class QueryTask:
    """Задача для обработки"""
    query: str
    index: int
    original_query: Optional[str] = None  # Если запрос был изменен (например, добавлен site:)


class AsyncQueueSERPClient:
    """
    Асинхронный клиент с общей очередью запросов
    
    Все прокси работают параллельно, каждый берет запросы из общей очереди.
    Логика: отправил → получил → пошел дальше
    Rate limit: 30 запросов в секунду на IP
    """
    
    def __init__(
        self,
        api_key: str,
        lr: int = 213,
        requests_per_second: float = 30.0,
        initial_delay: float = 1.0,
        retry_delay: float = 1.0,
        max_attempts: int = 50,
        device: str = 'desktop',
        proxies: Optional[List[str]] = None,
        proxy_file: Optional[str] = None,
        silent: bool = False
    ):
        """
        Args:
            api_key: API ключ xmlstock (user:key)
            lr: Регион
            requests_per_second: Максимум запросов в секунду на IP (по умолчанию 30)
            initial_delay: Задержка перед первой проверкой (сек)
            retry_delay: Задержка между повторами (сек)
            max_attempts: Макс попыток получения результата
            device: Устройство (desktop, mobile, tablet, iphone, android)
            proxies: Список прокси в формате ['http://user:pass@ip:port', ...]
            proxy_file: Путь к файлу с прокси (по одному на строку)
            silent: Не выводить сообщения
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.requests_per_second = requests_per_second
        self.initial_delay = initial_delay
        self.retry_delay = retry_delay
        self.max_attempts = max_attempts
        self.silent = silent
        
        # Парсинг ключа
        if ':' in api_key:
            self.user, self.key = api_key.split(':', 1)
        else:
            self.user = self.key = api_key
        
        self.url = "https://xmlstock.com/yandex/xml/"
        
        # Менеджер прокси
        self.proxy_manager = ProxyManager(proxies=proxies, proxy_file=proxy_file, silent=silent)
        proxy_count = self.proxy_manager.get_proxy_count()
        
        if proxy_count == 0:
            raise ValueError("Необходимо указать хотя бы один прокси")
        
        # Rate limiter для каждого прокси (30 запросов/сек)
        self.rate_limiters = {}
        for proxy_url in self.proxy_manager.proxies:
            self.rate_limiters[proxy_url] = RateLimiter(requests_per_second)
        
        # Executor для синхронных запросов
        self.executor_manager = ExecutorManager(max_workers=proxy_count * 10)
        
        # Статистика
        self.stats = {
            'total': 0,
            'sent': 0,
            'completed': 0,
            'failed_send': 0,
            'failed_fetch': 0
        }
    
    async def close(self):
        """Закрыть executor"""
        await self.executor_manager.close()
    
    async def process_queries_batch(
        self,
        queries: List[str],
        progress_callback: Optional[Callable] = None,
        on_req_id_received: Optional[Callable] = None,
        on_result_completed: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Обработка запросов через общую очередь
        
        Все прокси работают параллельно, каждый берет запросы из общей очереди.
        Логика: отправил → получил → пошел дальше
        """
        total = len(queries)
        self.stats['total'] = total
        self.stats['sent'] = 0
        self.stats['completed'] = 0
        self.stats['failed_send'] = 0
        self.stats['failed_fetch'] = 0
        
        proxy_count = self.proxy_manager.get_proxy_count()
        
        if not self.silent:
            print(f"\n⚡ STREAMING РЕЖИМ (Гибридный): {proxy_count} прокси работают асинхронно")
            print(f"   Общая очередь запросов: {total} запросов")
            print(f"   Rate limit: {self.requests_per_second} запросов в секунду на IP")
            print(f"   Гибридный режим: результат приходит сразу или ошибки 210/202")
            print(f"   Каждый прокси отправляет запросы непрерывно друг за другом")
            print(f"   Ошибки 210: повтор через 5-10 сек, ошибки 202: повтор через 10-20 сек")
            print(f"   Все прокси берут запросы из общей очереди параллельно")
            print()
        
        # Общая очередь запросов
        queue = asyncio.Queue()
        for idx, query in enumerate(queries, 1):
            await queue.put(QueryTask(query=query, index=idx))
        
        # Результаты
        all_results = []
        all_failed_send = []
        all_failed_fetch = []
        
        # Lock для статистики
        stats_lock = asyncio.Lock()
        
        async def process_query_with_proxy(proxy_url: str):
            """Обработать запросы через конкретный прокси - streaming режим"""
            proxy_dict = {
                'http': proxy_url,
                'https': proxy_url
            }
            rate_limiter = self.rate_limiters[proxy_url]
            
            # Словарь для отслеживания отправленных запросов: req_id -> {query, task, sent_at}
            pending_requests = {}
            
            async def send_requests_stream():
                """Непрерывно отправлять запросы друг за другом"""
                while True:
                    try:
                        # Берем запрос из очереди
                        try:
                            task = await asyncio.wait_for(queue.get(), timeout=0.1)
                        except asyncio.TimeoutError:
                            if queue.empty():
                                break
                            continue
                        
                        query = task.query
                        
                        # Rate limit для этого прокси
                        await rate_limiter.wait_for_rate_limit()
                        await RateLimiter.check_and_wait_for_503()
                        
                        # Отправляем запрос с delayed=0 (результат приходит сразу)
                        def send_request():
                            params = {
                                'user': self.user,
                                'key': self.key,
                                'query': query,
                                'lr': self.lr,
                                'device': self.device,
                                'groupby': 'attr=d.mode=deep.groups-on-page=20.docs-in-group=1',
                                'maxpassages': 2,
                                'filter': 'moderate',
                                # Гибридный режим включен по умолчанию (delayed не требуется)
                            }
                            try:
                                response = requests.get(
                                    self.url,
                                    params=params,
                                    timeout=(30, 30),  # Увеличиваем таймаут для delayed=0
                                    proxies=proxy_dict
                                )
                                if response.status_code == 200:
                                    xml_text = response.text
                                    
                                    # Проверяем на ошибки гибридного режима
                                    if '<error' in xml_text:
                                        error_match = re.search(r'<error[^>]*code="([^"]*)"', xml_text)
                                        if error_match:
                                            error_code = error_match.group(1)
                                            error_msg_match = re.search(r'<error[^>]*>([^<]+)</error>', xml_text)
                                            error_msg = error_msg_match.group(1) if error_msg_match else 'Unknown error'
                                            
                                            if error_code == '210':
                                                # Запрос поставлен в очередь - повторить через 5-10 секунд
                                                return {'status': 'queued', 'query': query, 'error_code': '210', 'error': error_msg, 'task': task, 'retry_delay': 7.5}
                                            elif error_code == '202':
                                                # Запрос еще не обработан - повторить через 10-20 секунд
                                                return {'status': 'pending', 'query': query, 'error_code': '202', 'error': error_msg, 'task': task, 'retry_delay': 15.0}
                                            else:
                                                # Другая ошибка
                                                return {'status': 'error', 'query': query, 'error': error_msg, 'error_code': error_code, 'task': task}
                                    
                                    # Результат готов сразу
                                    return {
                                        'query': query,
                                        'status': 'completed',
                                        'xml_response': xml_text,
                                        'task': task
                                    }
                                
                                is_503 = response.status_code == 503
                                return {
                                    'query': query,
                                    'status': 'error',
                                    'error': f"HTTP {response.status_code}",
                                    'is_503': is_503,
                                    'task': task
                                }
                            except (ProxyError, ConnectTimeout, RequestsConnectionError) as e:
                                return {'query': query, 'status': 'proxy_error', 'error': f"Proxy error: {str(e)[:100]}", 'task': task}
                            except Exception as e:
                                return {'query': query, 'status': 'error', 'error': str(e)[:100], 'task': task}
                        
                        result = await asyncio.get_event_loop().run_in_executor(
                            self.executor_manager.executor, send_request
                        )
                        
                        if result.get('is_503'):
                            await RateLimiter.mark_503_error()
                        
                        async with stats_lock:
                            self.stats['sent'] += 1
                            sent_count = self.stats['sent']
                        
                        # Логируем прогресс отправки
                        if not self.silent and sent_count % 50 == 0:
                            print(f"   📤 Отправлено: {sent_count}/{total} запросов")
                        
                        # Обрабатываем результат сразу
                        if result.get('status') == 'completed':
                            async with stats_lock:
                                self.stats['completed'] += 1
                            all_results.append(result)
                            
                            if on_result_completed:
                                try:
                                    on_result_completed(result)
                                except Exception as e:
                                    if not self.silent:
                                        print(f"   ⚠️  Ошибка в callback для '{query[:50]}...': {e}")
                            
                            if progress_callback:
                                progress_callback(self.stats['completed'], total, query, 'completed')
                            
                            queue.task_done()
                        
                        elif result.get('status') == 'queued':
                            # Ошибка 210 - запрос поставлен в очередь, повторить через 5-10 секунд
                            retry_delay = result.get('retry_delay', 7.5)
                            req_id = f"queued_{int(time.time() * 1000)}"
                            pending_requests[req_id] = {
                                'query': query,
                                'task': task,
                                'sent_at': time.time(),
                                'retry_delay': retry_delay,
                                'attempt': 0,
                                'error_code': '210'
                            }
                        
                        elif result.get('status') == 'pending':
                            # Ошибка 202 - запрос еще не обработан, повторить через 10-20 секунд
                            retry_delay = result.get('retry_delay', 15.0)
                            req_id = f"pending_{int(time.time() * 1000)}"
                            pending_requests[req_id] = {
                                'query': query,
                                'task': task,
                                'sent_at': time.time(),
                                'retry_delay': retry_delay,
                                'attempt': 0,
                                'error_code': '202'
                            }
                        
                        elif result.get('status') == 'proxy_error':
                            async with stats_lock:
                                self.stats['failed_send'] += 1
                            all_failed_send.append(result)
                            if progress_callback:
                                progress_callback(self.stats['completed'], total, query, 'failed_send')
                            queue.task_done()
                        
                        else:
                            # Ошибка
                            async with stats_lock:
                                self.stats['failed_send'] += 1
                            all_failed_send.append(result)
                            if progress_callback:
                                progress_callback(self.stats['completed'], total, query, 'failed_send')
                            queue.task_done()
                    
                    except Exception as e:
                        if not self.silent:
                            print(f"   ⚠️  Ошибка отправки через прокси {proxy_url[:30]}...: {e}")
                        continue
            
            async def fetch_results_stream():
                """Повторять запросы с ошибками 210/202 согласно документации"""
                while True:
                    if not pending_requests:
                        # Если нет запросов и очередь пуста - завершаем
                        if queue.empty():
                            await asyncio.sleep(0.5)
                            if not pending_requests:
                                break
                        await asyncio.sleep(0.1)
                        continue
                    
                    # Обрабатываем pending запросы (ошибки 210/202)
                    req_ids_to_check = list(pending_requests.keys())
                    
                    async def retry_single_request(req_id):
                        """Повторить один запрос с ошибкой 210/202"""
                        req_data = pending_requests.get(req_id)
                        if not req_data:
                            return
                        
                        query = req_data['query']
                        task_item = req_data['task']
                        sent_at = req_data['sent_at']
                        retry_delay = req_data.get('retry_delay', 10.0)
                        attempt = req_data.get('attempt', 0) + 1
                        error_code = req_data.get('error_code', '202')
                        
                        # Проверяем, прошло ли достаточно времени для повтора
                        time_since_sent = time.time() - sent_at
                        if time_since_sent < retry_delay:
                            # Еще рано повторять
                            return
                        
                        # Обновляем попытку
                        req_data['attempt'] = attempt
                        req_data['sent_at'] = time.time()
                        
                        # Повторяем запрос
                        def retry_request():
                            params = {
                                'user': self.user,
                                'key': self.key,
                                'query': query,
                                'lr': self.lr,
                                'device': self.device,
                                'groupby': 'attr=d.mode=deep.groups-on-page=20.docs-in-group=1',
                                'maxpassages': 2,
                                'filter': 'moderate',
                                'delayed': '0'
                            }
                            try:
                                response = requests.get(
                                    self.url,
                                    params=params,
                                    timeout=(30, 30),
                                    proxies=proxy_dict
                                )
                                if response.status_code == 200:
                                    xml_text = response.text
                                    
                                    if '<error' in xml_text:
                                        error_match = re.search(r'<error[^>]*code="([^"]*)"', xml_text)
                                        if error_match:
                                            error_code_new = error_match.group(1)
                                            error_msg_match = re.search(r'<error[^>]*>([^<]+)</error>', xml_text)
                                            error_msg = error_msg_match.group(1) if error_msg_match else 'Unknown error'
                                            
                                            if error_code_new == '210':
                                                return {'status': 'queued', 'query': query, 'error_code': '210', 'error': error_msg, 'task': task_item, 'retry_delay': 7.5}
                                            elif error_code_new == '202':
                                                return {'status': 'pending', 'query': query, 'error_code': '202', 'error': error_msg, 'task': task_item, 'retry_delay': 15.0}
                                            else:
                                                return {'status': 'error', 'query': query, 'error': error_msg, 'error_code': error_code_new, 'task': task_item}
                                    
                                    # Результат готов
                                    return {
                                        'query': query,
                                        'status': 'completed',
                                        'xml_response': xml_text,
                                        'task': task_item
                                    }
                                
                                is_503 = response.status_code == 503
                                return {
                                    'query': query,
                                    'status': 'error',
                                    'error': f"HTTP {response.status_code}",
                                    'is_503': is_503,
                                    'task': task_item
                                }
                            except (ProxyError, ConnectTimeout, RequestsConnectionError) as e:
                                return {'query': query, 'status': 'proxy_error', 'error': f"Proxy error: {str(e)[:100]}", 'task': task_item}
                            except Exception as e:
                                return {'query': query, 'status': 'error', 'error': str(e)[:100], 'task': task_item}
                        
                        retry_result = await asyncio.get_event_loop().run_in_executor(
                            self.executor_manager.executor, retry_request
                        )
                        
                        if retry_result.get('is_503'):
                            await RateLimiter.mark_503_error()
                        
                        if retry_result.get('status') == 'completed':
                            # Результат получен
                            pending_requests.pop(req_id, None)
                            
                            async with stats_lock:
                                self.stats['completed'] += 1
                            
                            all_results.append(retry_result)
                            
                            if on_result_completed:
                                try:
                                    on_result_completed(retry_result)
                                except Exception as e:
                                    if not self.silent:
                                        print(f"   ⚠️  Ошибка в callback для '{query[:50]}...': {e}")
                            
                            if progress_callback:
                                progress_callback(self.stats['completed'], total, query, 'completed')
                            
                            if task_item:
                                queue.task_done()
                        
                        elif retry_result.get('status') in ('queued', 'pending'):
                            # Все еще в очереди - обновляем данные для следующей попытки
                            req_data['retry_delay'] = retry_result.get('retry_delay', retry_delay)
                            req_data['error_code'] = retry_result.get('error_code', error_code)
                            req_data['sent_at'] = time.time()
                        
                        elif retry_result.get('status') == 'error':
                            # Ошибка после нескольких попыток
                            if attempt >= 10:  # Максимум 10 попыток
                                pending_requests.pop(req_id, None)
                                async with stats_lock:
                                    self.stats['failed_fetch'] += 1
                                all_failed_fetch.append(retry_result)
                                if progress_callback:
                                    progress_callback(self.stats['completed'], total, query, 'failed_fetch')
                                if task_item:
                                    queue.task_done()
                    
                    if req_ids_to_check:
                        await asyncio.gather(*[retry_single_request(req_id) for req_id in req_ids_to_check], return_exceptions=True)
                    
                    await asyncio.sleep(0.5)  # Проверяем каждые 0.5 секунды
            
            # Запускаем отправку и обработку pending параллельно
            await asyncio.gather(
                send_requests_stream(),
                fetch_results_stream(),
                return_exceptions=True
            )
        
        # Запускаем обработку через все прокси параллельно
        proxy_tasks = [
            asyncio.create_task(process_query_with_proxy(proxy_url))
            for proxy_url in self.proxy_manager.proxies
        ]
        
        # Ждем завершения всех задач
        await asyncio.gather(*proxy_tasks, return_exceptions=True)
        
        # Ждем завершения всех задач из очереди
        await queue.join()
        
        if not self.silent:
            print(f"\n{'='*80}")
            print(f"✅ Загрузка завершена")
            print(f"   Всего: {total}")
            print(f"   ✅ Успешно: {self.stats['completed']}")
            print(f"   ❌ Ошибок отправки: {self.stats['failed_send']}")
            print(f"   ❌ Ошибок получения: {self.stats['failed_fetch']}")
            print(f"{'='*80}\n")
        
        return {
            'results': all_results + all_failed_fetch + all_failed_send,
            'stats': {
                'total': total,
                'sent': self.stats['sent'],
                'completed': self.stats['completed'],
                'failed_send': self.stats['failed_send'],
                'failed_fetch': self.stats['failed_fetch']
            }
        }


__all__ = ['AsyncQueueSERPClient', 'QueryTask']

