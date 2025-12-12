"""
Простой клиент для парсинга SERP данных
Последовательная обработка: отправил → получил → следующий запрос
Максимум 50 параллельных запросов на один IP
"""

import asyncio
import requests
import re
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

from .batch.rate_limiter import RateLimiter
from .sync_batch.executor_manager import ExecutorManager
from .batch.proxy_manager import ProxyManager


@dataclass
class PendingRequest:
    """Отложенный запрос"""
    query: str
    req_id: str
    sent_at: datetime
    attempts: int = 0
    last_error: Optional[str] = None


class SyncBatchSERPClient:
    """
    Простой клиент: отправил → получил → следующий запрос
    Максимум 50 параллельных запросов на один IP
    """
    
    def __init__(
        self,
        api_key: str,
        lr: int = 213,
        max_concurrent_send: int = 50,
        max_concurrent_fetch: int = 50,
        initial_delay: int = 2,
        retry_delay: int = 2,
        max_attempts: int = 50,
        requests_per_second: float = 50.0,
        device: str = 'desktop',
        proxies: Optional[List[str]] = None,
        proxy_file: Optional[str] = None,
        silent: bool = False
    ):
        """
        Args:
            api_key: API ключ xmlstock (user:key)
            lr: Регион
            max_concurrent_send: Макс параллельных отправок (максимум 50 на IP)
            max_concurrent_fetch: Макс параллельных получений (максимум 50 на IP)
            initial_delay: Задержка перед первой проверкой (сек)
            retry_delay: Задержка между повторами (сек)
            max_attempts: Макс попыток получения результата
            requests_per_second: Максимум запросов в секунду
            device: Устройство (desktop, mobile, tablet, iphone, android)
            proxies: Список прокси в формате ['http://user:pass@ip:port', ...]
            proxy_file: Путь к файлу с прокси (по одному на строку)
            silent: Не выводить сообщения о загрузке прокси и режиме работы
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.max_concurrent_send = min(max_concurrent_send, 50)  # Максимум 50 на IP
        self.max_concurrent_fetch = min(max_concurrent_fetch, 50)  # Максимум 50 на IP
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
        
        # Инициализация модулей
        self.rate_limiter = RateLimiter(requests_per_second)
        self.executor_manager = ExecutorManager(max_workers=60)  # Немного больше чем max_concurrent
        
        # Менеджер прокси
        self.proxy_manager = ProxyManager(proxies=proxies, proxy_file=proxy_file, silent=silent)
    
    async def close(self):
        """Закрыть executor"""
        await self.executor_manager.close()
    
    async def process_queries_batch(
        self,
        queries: List[str],
        progress_callback: Optional[Callable] = None,
        on_req_id_received: Optional[Callable] = None,
        on_result_completed: Optional[Callable] = None,
        batch_size: int = 50,
        completion_threshold: float = 0.95
    ) -> Dict[str, Any]:
        """
        Простая последовательная обработка: отправил → получил → следующий запрос
        Rate limit: максимум 50 запросов в секунду на прокси
        """
        import requests
        import re
        from requests.exceptions import ProxyError, ConnectTimeout, ConnectionError as RequestsConnectionError
        
        total = len(queries)
        if not self.silent:
            print(f"\n⚡ SIMPLE MODE (delayed=0): {total} запросов")
            print(f"   🚀 Rate limit: {self.requests_per_second} запросов/сек на прокси")
            print(f"   🔄 Параллельность: до {self.max_concurrent_send} одновременных запросов")
            print(f"   ⚡ Гибридный режим: результат приходит сразу в ответе")
            print()
        
        all_results = []
        all_failed_send = []
        all_failed_fetch = []
        total_sent = 0
        total_completed = 0
        
        # Семафор для ограничения параллельности (чтобы не перегрузить систему)
        semaphore = asyncio.Semaphore(self.max_concurrent_send)
        
        async def process_single_query(query: str, index: int):
            """Обработать один запрос полностью: отправил → получил → записал"""
            nonlocal total_sent, total_completed
            
            async with semaphore:
                # Получаем прокси ОДИН РАЗ для всего запроса
                proxies = self.proxy_manager.get_proxy(strategy='round_robin')
                
                # Rate limit: максимум 50 запросов в секунду на прокси
                await self.rate_limiter.wait_for_rate_limit()
                
                # Отправка запроса с delayed=0 (результат приходит сразу)
                def send_request(q):
                    params = {
                        'user': self.user,
                        'key': self.key,
                        'query': q,
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
                            proxies=proxies
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
                                        return {'query': q, 'status': 'queued', 'error_code': '210', 'error': error_msg, 'proxies': proxies, 'retry_delay': 7.5}
                                    elif error_code == '202':
                                        # Запрос еще не обработан - повторить через 10-20 секунд
                                        return {'query': q, 'status': 'pending', 'error_code': '202', 'error': error_msg, 'proxies': proxies, 'retry_delay': 15.0}
                                    else:
                                        # Другая ошибка
                                        return {'query': q, 'status': 'error', 'error': error_msg, 'error_code': error_code}
                            
                            # Результат готов сразу
                            return {
                                'query': q,
                                'status': 'completed',
                                'xml_response': xml_text,
                                'proxies': proxies
                            }
                        
                        return {'query': q, 'status': 'error', 'error': f"HTTP {response.status_code}"}
                    except (ProxyError, ConnectTimeout, RequestsConnectionError) as e:
                        return {'query': q, 'status': 'proxy_error', 'error': f"Proxy error: {str(e)[:100]}"}
                    except Exception as e:
                        return {'query': q, 'status': 'error', 'error': str(e)[:100]}
                
                result = await asyncio.get_event_loop().run_in_executor(
                    self.executor_manager.executor, send_request, query
                )
                
                if result.get('status') == 'error':
                    all_failed_send.append(result)
                    if progress_callback:
                        progress_callback(total_completed, total, query, 'failed_send')
                    return
                
                total_sent += 1
                
                # Обрабатываем результат сразу
                if result.get('status') == 'completed':
                    total_completed += 1
                    all_results.append(result)
                    
                    if on_result_completed:
                        try:
                            on_result_completed(result)
                        except Exception as e:
                            if not self.silent:
                                print(f"   ⚠️  Ошибка в callback для '{query[:50]}...': {e}")
                    
                    if progress_callback:
                        progress_callback(total_completed, total, query, 'completed')
                
                elif result.get('status') in ('queued', 'pending'):
                    # Ошибка 210 или 202 - повторяем запрос с правильными задержками
                    error_code = result.get('error_code', '202')
                    retry_delay = result.get('retry_delay', 15.0)
                    proxies = result.get('proxies')
                    
                    # Ждем перед повтором
                    await asyncio.sleep(retry_delay)
                    
                    attempt = 0
                    max_retry_attempts = 20  # Максимум попыток для ошибок 210/202
                    
                    while attempt < max_retry_attempts:
                        attempt += 1
                        
                        # Повторяем запрос (не по req_id, а полностью)
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
                                # Гибридный режим включен по умолчанию
                            }
                            try:
                                response = requests.get(
                                    self.url, 
                                    params=params, 
                                    timeout=(30, 30),
                                    proxies=proxies
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
                                                return {'status': 'queued', 'query': query, 'error_code': '210', 'error': error_msg, 'retry_delay': 7.5}
                                            elif error_code_new == '202':
                                                return {'status': 'pending', 'query': query, 'error_code': '202', 'error': error_msg, 'retry_delay': 15.0}
                                            else:
                                                return {'query': query, 'status': 'error', 'error': error_msg, 'error_code': error_code_new}
                                    
                                    # Результат готов
                                    return {
                                        'query': query,
                                        'status': 'completed',
                                        'xml_response': xml_text
                                    }
                                return {'query': query, 'status': 'error', 'error': f"HTTP {response.status_code}"}
                            except (ProxyError, ConnectTimeout, RequestsConnectionError) as e:
                                return {'query': query, 'status': 'proxy_error', 'error': str(e)[:100]}
                            except Exception as e:
                                return {'query': query, 'status': 'error', 'error': str(e)[:100]}
                        
                        retry_result = await asyncio.get_event_loop().run_in_executor(
                            self.executor_manager.executor, retry_request
                        )
                        
                        if retry_result.get('status') == 'completed':
                            total_completed += 1
                            all_results.append(retry_result)
                            
                            if on_result_completed:
                                try:
                                    on_result_completed(retry_result)
                                except Exception as e:
                                    if not self.silent:
                                        print(f"   ⚠️  Ошибка в callback для '{query[:50]}...': {e}")
                            
                            if progress_callback:
                                progress_callback(total_completed, total, query, 'completed')
                            return
                        
                        elif retry_result.get('status') in ('queued', 'pending'):
                            # Все еще в очереди - обновляем задержку и повторяем
                            retry_delay = retry_result.get('retry_delay', retry_delay)
                            error_code = retry_result.get('error_code', error_code)
                            await asyncio.sleep(retry_delay)
                            continue
                        
                        else:
                            # Ошибка
                            all_failed_fetch.append(retry_result)
                            if progress_callback:
                                progress_callback(total_completed, total, query, 'failed_fetch')
                            return
                    
                    # Достигнут лимит попыток
                    all_failed_fetch.append({
                        'query': query,
                        'status': 'error',
                        'error': f'Превышен лимит попыток ({max_retry_attempts}) для ошибки {error_code}'
                    })
                    if progress_callback:
                        progress_callback(total_completed, total, query, 'failed_fetch')
                
                elif result.get('status') == 'proxy_error':
                    all_failed_send.append(result)
                    if progress_callback:
                        progress_callback(total_completed, total, query, 'failed_send')
        
        # Запускаем обработку всех запросов параллельно (с ограничением через семафор)
        tasks = [
            asyncio.create_task(process_single_query(query, i))
            for i, query in enumerate(queries, 1)
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Подсчитываем статистику
        real_failures = [f for f in all_failed_fetch if f.get('status') in ('failed', 'error')]
        
        if not self.silent:
            print(f"\n{'='*80}")
            print(f"✅ Загрузка завершена")
            print(f"   Всего: {total}")
            print(f"   ✅ Успешно: {len(all_results)}")
            print(f"   ❌ Ошибок отправки: {len(all_failed_send)}")
            print(f"   ❌ Ошибок получения: {len(real_failures)}")
            print(f"{'='*80}\n")
        
        return {
            'results': all_results + all_failed_fetch + all_failed_send,
            'stats': {
                'total': total,
                'sent': total_sent,
                'completed': len(all_results),
                'failed_send': len(all_failed_send),
                'failed_fetch': len([f for f in all_failed_fetch if f.get('status') == 'failed'])
            }
        }


__all__ = ['SyncBatchSERPClient', 'PendingRequest']
