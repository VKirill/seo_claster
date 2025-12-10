"""
Rate limiting для SERP запросов
"""

import asyncio
import time
from typing import Optional


class RateLimiter:
    """Rate limiter для соблюдения лимитов запросов"""
    
    # Глобальный флаг перегрузки сервера (503)
    # Если сервер вернул 503, все запросы ждут 60 сек перед следующей попыткой
    _last_503_time: Optional[float] = None
    _503_lock: Optional[asyncio.Lock] = None
    
    def __init__(self, requests_per_second: float = 50.0):
        """
        Args:
            requests_per_second: Максимум запросов в секунду
        """
        self.requests_per_second = requests_per_second
        self.min_request_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.01
        self._last_request_time = 0.0
        self._rate_limit_lock = asyncio.Lock()
    
    @classmethod
    def _get_503_lock(cls) -> asyncio.Lock:
        """Получить или создать Lock для синхронизации"""
        if cls._503_lock is None:
            cls._503_lock = asyncio.Lock()
        return cls._503_lock
    
    @classmethod
    async def check_and_wait_for_503(cls):
        """Проверить, был ли недавно 503, и если да - подождать"""
        async with cls._get_503_lock():
            if cls._last_503_time is not None:
                time_since_503 = time.time() - cls._last_503_time
                if time_since_503 < 60:
                    wait_time = 60 - time_since_503
                    print(f"   ⚠️  Сервер был перегружен (503) {time_since_503:.1f} сек назад. Ожидание {wait_time:.1f} сек...")
                    await asyncio.sleep(wait_time)
                    cls._last_503_time = None  # Сбрасываем после ожидания
    
    @classmethod
    async def mark_503_error(cls):
        """Отметить что сервер вернул 503"""
        async with cls._get_503_lock():
            cls._last_503_time = time.time()
    
    @classmethod
    def get_time_since_503(cls) -> Optional[float]:
        """Получить время с последнего 503 (в секундах) или None если не было"""
        if cls._last_503_time is None:
            return None
        return time.time() - cls._last_503_time
    
    @classmethod
    async def reset_503_flag(cls):
        """Сбросить флаг 503"""
        async with cls._get_503_lock():
            cls._last_503_time = None
    
    async def wait_for_rate_limit(self):
        """Ожидание для соблюдения rate limit (не более requests_per_second запросов/сек)"""
        async with self._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self._last_request_time
            
            if time_since_last < self.min_request_interval:
                wait_time = self.min_request_interval - time_since_last
                await asyncio.sleep(wait_time)
            
            self._last_request_time = time.time()

