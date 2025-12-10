"""
Async Batch SERP Client
Массовая отправка запросов в асинхронном режиме xmlstock

Workflow:
1. Отправляем ВСЕ запросы с delayed=1 → получаем все req_id
2. Ждём 10-20 секунд
3. Параллельно запрашиваем результаты по всем req_id
4. Повторяем для тех что ещё не готовы (код 202)
"""

import asyncio
import time
from typing import List, Dict, Any, Optional, Callable

from .models.pending_request import PendingRequest
from .batch.rate_limiter import RateLimiter
from .batch.session_manager import SessionManager
from .batch.request_sender import RequestSender
from .batch.result_fetcher import ResultFetcher


class AsyncBatchSERPClient:
    """
    Массовая отправка и получение SERP данных в асинхронном режиме
    
    Преимущества:
    - Отправка 1000+ запросов за несколько секунд
    - Параллельное получение результатов
    - Автоматические повторы для незавершённых (202)
    - Сохранение req_id в Master DB для восстановления
    """
    
    def __init__(
        self,
        api_key: str,
        lr: int = 213,
        max_concurrent_send: int = 10,
        max_concurrent_fetch: int = 20,
        initial_delay: int = 10,
        retry_delay: int = 5,
        max_attempts: int = 100,
        requests_per_second: float = 50.0,
        device: str = 'desktop',
        site: str = None
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
            site: Домен для фильтрации (site:domain.ru)
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.site = site
        self.max_concurrent_send = max_concurrent_send
        self.max_concurrent_fetch = max_concurrent_fetch
        self.initial_delay = initial_delay
        self.retry_delay = retry_delay
        self.max_attempts = max_attempts
        
        # Парсинг ключа
        if ':' in api_key:
            self.user, self.key = api_key.split(':', 1)
        else:
            self.user = api_key
            self.key = api_key
        
        self.url = "https://xmlstock.com/yandex/xml/"
        
        # Инициализация модулей
        self.rate_limiter = RateLimiter(requests_per_second)
        self.session_manager = SessionManager()
        self.request_sender = RequestSender(
            user=self.user,
            key=self.key,
            lr=self.lr,
            url=self.url,
            rate_limiter=self.rate_limiter,
            session_manager=self.session_manager,
            device=self.device
        )
        self.result_fetcher = ResultFetcher(
            user=self.user,
            key=self.key,
            url=self.url,
            rate_limiter=self.rate_limiter,
            session_manager=self.session_manager
        )
    
    async def close(self):
        """Закрыть сессию и connector"""
        await self.session_manager.close()
    
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
        await self.session_manager.ensure_session()
        
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
        fetch_semaphore = asyncio.Semaphore(self.max_concurrent_fetch)
        
        async def process_single_query(query: str, index: int):
            """Обработать один запрос: отправить → получить → вернуть результат"""
            nonlocal total_sent, total_completed
            
            # ЭТАП 1: Отправка запроса
            try:
                async with send_semaphore:
                    pending = await self.request_sender.send_delayed_request(
                        query,
                        index,
                        total,
                        send_semaphore,
                        progress_callback,
                        on_req_id_received
                    )
                    
                    if not isinstance(pending, PendingRequest):
                        # Ошибка отправки
                        all_failed_send.append({
                            'query': query,
                            'error': str(pending) if pending else 'Unknown error'
                        })
                        if progress_callback:
                            progress_callback(total_completed, total, query, 'failed_send')
                        return
                    
                    total_sent += 1
                    
                    # ЭТАП 2: Ожидание перед получением результата
                    await asyncio.sleep(self.initial_delay)
                    
                    # ЭТАП 3: Получение результата (с повторами)
                    async with fetch_semaphore:
                        for attempt in range(self.max_attempts):
                            result = await self.result_fetcher.fetch_result_by_req_id(
                                pending,
                                fetch_semaphore,
                                progress_callback
                            )
                            
                            if isinstance(result, dict):
                                if result.get('status') == 'completed':
                                    # Успешно получили результат
                                    total_completed += 1
                                    all_results.append(result)
                                    
                                    # Вызываем callback для немедленной обработки
                                    if on_result_completed:
                                        try:
                                            on_result_completed(result)
                                        except Exception as e:
                                            print(f"   ⚠️  Ошибка в callback обработки результата для '{query[:50]}...': {e}")
                                    
                                    if progress_callback:
                                        progress_callback(total_completed, total, query, 'completed')
                                    return
                                
                                elif result.get('status') == 'pending':
                                    # Еще не готово - ждем и повторяем
                                    if attempt < self.max_attempts - 1:
                                        await asyncio.sleep(self.retry_delay)
                                        continue
                                
                                elif result.get('status') == 'retry_503':
                                    # 503 ошибка - ждем дольше
                                    if attempt < self.max_attempts - 1:
                                        await asyncio.sleep(60)
                                        continue
                                
                                else:
                                    # Ошибка получения
                                    all_failed_fetch.append(result)
                                    if progress_callback:
                                        progress_callback(total_completed, total, query, 'failed_fetch')
                                    return
                            
                            elif isinstance(result, Exception):
                                # Исключение при получении
                                all_failed_fetch.append({
                                    'query': query,
                                    'req_id': pending.req_id,
                                    'status': 'error',
                                    'error': str(result)
                                })
                                if progress_callback:
                                    progress_callback(total_completed, total, query, 'failed_fetch')
                                return
                        
                        # Не получили результат после всех попыток
                        all_failed_fetch.append({
                            'query': query,
                            'req_id': pending.req_id,
                            'status': 'failed',
                            'error': f"Not ready after {self.max_attempts} attempts"
                        })
                        if progress_callback:
                            progress_callback(total_completed, total, query, 'failed_fetch')
            
            except Exception as e:
                # Ошибка при обработке
                all_failed_send.append({
                    'query': query,
                    'error': str(e)
                })
                if progress_callback:
                    progress_callback(total_completed, total, query, 'failed_send')
        
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


__all__ = ['AsyncBatchSERPClient', 'PendingRequest']
