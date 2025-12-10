"""
Управление ThreadPoolExecutor для синхронных запросов
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Optional


class ExecutorManager:
    """Менеджер ThreadPoolExecutor"""
    
    def __init__(self, max_workers: int = 20):
        """
        Args:
            max_workers: Максимальное количество потоков
        """
        self._executor: Optional[ThreadPoolExecutor] = None
        self.max_workers = max_workers
    
    @property
    def executor(self) -> ThreadPoolExecutor:
        """Получить или создать executor"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._executor
    
    async def close(self):
        """Закрыть executor"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

