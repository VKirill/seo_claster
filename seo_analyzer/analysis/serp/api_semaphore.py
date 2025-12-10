"""
Global API Semaphore
Глобальный семафор для контроля параллельности запросов к XMLStock API
"""

import asyncio


class APIRequestSemaphore:
    """
    Глобальный семафор для ограничения количества одновременных запросов к API
    
    Согласно рекомендациям XMLStock:
    - Не более 50 одновременных потоков
    - Отправлять следующий запрос сразу после получения результата на предыдущий
    
    Этот семафор работает на уровне ВСЕГО приложения, не только одной группы.
    """
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls, max_concurrent: int = 50):
        """Singleton pattern для глобального семафора"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, max_concurrent: int = 50):
        """
        Инициализация семафора
        
        Args:
            max_concurrent: Максимальное количество одновременных запросов (по умолчанию 50)
        """
        if self._initialized:
            return
        
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_requests = 0
        self._total_requests = 0
        self._initialized = True
        
        print(f"🔒 Глобальный семафор API: максимум {max_concurrent} параллельных запросов")
    
    @property
    def semaphore(self):
        """Получить семафор для использования"""
        return self._semaphore
    
    async def acquire_slot(self):
        """Захватить слот для запроса"""
        await self._semaphore.acquire()
        self._active_requests += 1
        self._total_requests += 1
    
    def release_slot(self):
        """Освободить слот после запроса"""
        self._semaphore.release()
        self._active_requests -= 1
    
    async def __aenter__(self):
        """Вход в async context manager"""
        await self._semaphore.acquire()
        self._active_requests += 1
        self._total_requests += 1
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из async context manager"""
        self._semaphore.release()
        self._active_requests -= 1
        return False
    
    def get_stats(self):
        """Получить статистику использования"""
        return {
            'max_concurrent': self._max_concurrent,
            'active_requests': self._active_requests,
            'total_requests': self._total_requests,
            'available_slots': self._max_concurrent - self._active_requests
        }
    
    @classmethod
    def reset(cls):
        """Сброс singleton (для тестов)"""
        cls._instance = None


# Глобальный экземпляр
_global_semaphore = None


def get_api_semaphore(max_concurrent: int = 50) -> APIRequestSemaphore:
    """
    Получить глобальный семафор API
    
    Args:
        max_concurrent: Максимальное количество одновременных запросов
        
    Returns:
        Глобальный экземпляр APIRequestSemaphore
    """
    global _global_semaphore
    if _global_semaphore is None:
        _global_semaphore = APIRequestSemaphore(max_concurrent)
    return _global_semaphore

