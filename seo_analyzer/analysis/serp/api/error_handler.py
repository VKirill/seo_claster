"""
Обработка ошибок API (503, таймауты и т.д.)
"""

import asyncio
import time
from typing import Optional


class ErrorHandler:
    """Обработчик ошибок API"""
    
    # Глобальный флаг перегрузки сервера (503)
    _last_503_time: Optional[float] = None
    _503_lock: Optional[asyncio.Lock] = None
    
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
    
    @staticmethod
    def is_503_error(error_text: str) -> bool:
        """Проверить, является ли ошибка 503"""
        return '503' in error_text or 'Service Temporarily Unavailable' in error_text

