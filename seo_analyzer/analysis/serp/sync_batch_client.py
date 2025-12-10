"""
Hybrid Batch SERP Client
Использует синхронный requests внутри async через executor
Решает проблемы с aiohttp Connection closed
Фасад для модулей синхронной обработки
"""

import asyncio
import time
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

from .batch.rate_limiter import RateLimiter
from .sync_batch.executor_manager import ExecutorManager


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
    Гибридный клиент: синхронный requests + async executor
    
    Преимущества:
    - Стабильность requests (нет проблем с Connection closed)
    - Контроль параллельности через async
    - Простота отладки
    """
    
    def __init__(
        self,
        api_key: str,
        lr: int = 213,
        max_concurrent_send: int = 10,
        max_concurrent_fetch: int = 20,
        initial_delay: int = 10,
        retry_delay: int = 10,
        max_attempts: int = 100,
        requests_per_second: float = 50.0,
        device: str = 'desktop'
    ):
        """
        Args:
            api_key: API ключ xmlstock (user:key)
            lr: Регион
            max_concurrent_send: Макс параллельных отправок
            max_concurrent_fetch: Макс параллельных получений
            initial_delay: Задержка перед первой проверкой (сек)
            retry_delay: Задержка между повторами (сек)
            max_attempts: Макс попыток получения результата
            requests_per_second: Максимум запросов в секунду
            device: Устройство (desktop, mobile, tablet, iphone, android)
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.max_concurrent_send = max_concurrent_send
        self.max_concurrent_fetch = max_concurrent_fetch
        self.initial_delay = initial_delay
        self.retry_delay = retry_delay
        self.max_attempts = max_attempts
        
        # Парсинг ключа
        if ':' in api_key:
            self.user, self.key = api_key.split(':', 1)
        else:
            self.user = self.key = api_key
        
        self.url = "https://xmlstock.com/yandex/xml/"
        
        # Инициализация модулей
        self.rate_limiter = RateLimiter(requests_per_second)
        self.executor_manager = ExecutorManager(max_workers=max(max_concurrent_send, max_concurrent_fetch))
    
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
        Streaming обработка запросов: каждый запрос обрабатывается независимо
        Отправил → Получил → Обработал → Сохранил → Следующий
        
        Args:
            queries: Список запросов
            progress_callback: Callback(current, total, query, status)
            on_req_id_received: Callback(query, req_id) при получении req_id
            on_result_completed: Callback(result_dict) при получении результата (для немедленной обработки)
            batch_size: Игнорируется (для совместимости)
            completion_threshold: Игнорируется (для совместимости)
            
        Returns:
            Dict с результатами и статистикой
        """
        import requests
        import re
        
        total = len(queries)
        print(f"\n🚀 STREAMING MODE: {total} запросов")
        print(f"   📦 Стратегия: ОТПРАВИЛ → ПОЛУЧИЛ → ОБРАБОТАЛ → СЛЕДУЮЩИЙ")
        print(f"   🔄 Параллельность: {self.max_concurrent_send} одновременных запросов")
        print(f"   ⏳ Задержка перед получением: {self.initial_delay} сек")
        print(f"   ⚡ Rate limit: {self.rate_limiter.requests_per_second:.0f} запросов/сек")
        print()
        
        all_results = []
        all_failed_send = []
        all_failed_fetch = []
        total_sent = 0
        total_completed = 0
        
        # Семафор для ограничения параллельности
        send_semaphore = asyncio.Semaphore(self.max_concurrent_send)
        
        async def process_single_query(query: str, index: int):
            """Обработать один запрос: отправить → получить → вернуть результат"""
            nonlocal total_sent, total_completed
            
            async with send_semaphore:
                # ЭТАП 1: Отправка запроса
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
                        'delayed': '1'
                    }
                    response = requests.get(self.url, params=params, timeout=10)
                    if response.status_code == 200:
                        xml_text = response.text
                        req_id_match = re.search(r'<req_id>([^<]+)</req_id>', xml_text)
                        if req_id_match:
                            return {'query': q, 'req_id': req_id_match.group(1)}
                    return {'query': q, 'error': f"HTTP {response.status_code}"}
                
                send_result = await asyncio.get_event_loop().run_in_executor(
                    self.executor_manager.executor, send_request, query
                )
                
                if 'error' in send_result:
                    all_failed_send.append(send_result)
                    if progress_callback:
                        progress_callback(total_completed, total, query, 'failed_send')
                    return
                
                req_id = send_result['req_id']
                total_sent += 1
                
                # Сохраняем req_id
                if on_req_id_received:
                    on_req_id_received(query, req_id)
                
                # ЭТАП 2: Ожидание перед получением результата
                await asyncio.sleep(self.initial_delay)
                
                # ЭТАП 3: Получение результата (с повторами)
                pending = PendingRequest(
                    query=query,
                    req_id=req_id,
                    sent_at=datetime.now()
                )
                
                for attempt in range(self.max_attempts):
                    def fetch_result(p):
                        params = {'user': self.user, 'key': self.key, 'req_id': p.req_id}
                        response = requests.get(self.url, params=params, timeout=10)
                        if response.status_code == 200:
                            xml_text = response.text
                            if '<error' in xml_text:
                                error_match = re.search(r'<error[^>]*code="([^"]*)"', xml_text)
                                if error_match and error_match.group(1) == '202':
                                    return {'status': 'pending'}
                            return {
                                'query': p.query,
                                'req_id': p.req_id,
                                'status': 'completed',
                                'xml_response': xml_text
                            }
                        return {'query': p.query, 'status': 'error', 'error': f"HTTP {response.status_code}"}
                    
                    fetch_result_data = await asyncio.get_event_loop().run_in_executor(
                        self.executor_manager.executor, fetch_result, pending
                    )
                    
                    if fetch_result_data.get('status') == 'completed':
                        # Успешно получили результат
                        total_completed += 1
                        all_results.append(fetch_result_data)
                        
                        # Вызываем callback для немедленной обработки
                        if on_result_completed:
                            try:
                                on_result_completed(fetch_result_data)
                            except Exception as e:
                                print(f"   ⚠️  Ошибка в callback обработки результата для '{query[:50]}...': {e}")
                        
                        if progress_callback:
                            progress_callback(total_completed, total, query, 'completed')
                        return
                    
                    elif fetch_result_data.get('status') == 'pending':
                        # Еще не готово - ждем и повторяем
                        if attempt < self.max_attempts - 1:
                            await asyncio.sleep(self.retry_delay)
                            continue
                    
                    else:
                        # Ошибка получения
                        all_failed_fetch.append(fetch_result_data)
                        if progress_callback:
                            progress_callback(total_completed, total, query, 'failed_fetch')
                        return
                
                # Не получили результат после всех попыток
                all_failed_fetch.append({
                    'query': query,
                    'req_id': req_id,
                    'status': 'failed',
                    'error': f"Not ready after {self.max_attempts} attempts"
                })
                if progress_callback:
                    progress_callback(total_completed, total, query, 'failed_fetch')
        
        # Запускаем обработку всех запросов параллельно
        tasks = []
        for i, query in enumerate(queries, 1):
            task = asyncio.create_task(process_single_query(query, i))
            tasks.append(task)
        
        # Ждем завершения всех задач
        await asyncio.gather(*tasks, return_exceptions=True)
        
        print(f"\n{'='*80}")
        print(f"✅ STREAMING загрузка завершена")
        print(f"   Всего: {total}")
        print(f"   Успешно: {len(all_results)}")
        print(f"   Ошибок отправки: {len(all_failed_send)}")
        print(f"   Ошибок получения: {len([f for f in all_failed_fetch if f.get('status') == 'failed'])}")
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
