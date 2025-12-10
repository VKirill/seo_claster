"""
Управление HTTP сессией для SERP запросов
"""

import asyncio
import aiohttp
from typing import Optional


class SessionManager:
    """Менеджер HTTP сессии"""
    
    def __init__(self):
        """Инициализация менеджера сессии"""
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
    
    async def ensure_session(self):
        """Создать сессию если нет (thread-safe)"""
        # Проверяем без блокировки для быстрого пути
        if self._session is not None and not self._session.closed:
            return
        
        # Используем блокировку для создания/закрытия сессии
        async with self._session_lock:
            # Двойная проверка после получения блокировки
            if self._session is not None and not self._session.closed:
                return
            
            # Закрываем старую сессию если есть (на всякий случай)
            if self._session is not None:
                try:
                    if not self._session.closed:
                        await self._session.close()
                    # Закрываем connector если есть
                    if hasattr(self._session, '_connector') and self._session._connector:
                        await self._session._connector.close()
                    # Даём время закрыться
                    await asyncio.sleep(0.25)
                except Exception:
                    # Игнорируем ошибки при закрытии
                    pass
                finally:
                    self._session = None
            
            # Создаём новую сессию с правильными настройками
            connector = aiohttp.TCPConnector(
                limit=100, 
                limit_per_host=50, 
                force_close=False,
                ttl_dns_cache=300,
                enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(
                connector=connector, 
                timeout=timeout,
                raise_for_status=False  # Не кидаем исключения на HTTP ошибки
            )
    
    async def close(self):
        """Закрыть сессию и connector (thread-safe)"""
        async with self._session_lock:
            if self._session:
                try:
                    # Сначала закрываем connector (освобождает соединения)
                    if hasattr(self._session, '_connector') and self._session._connector:
                        if not self._session._connector.closed:
                            await self._session._connector.close()
                    
                    # Затем закрываем саму сессию
                    if not self._session.closed:
                        await self._session.close()
                    
                    # Даём больше времени для полного закрытия всех соединений
                    await asyncio.sleep(0.5)
                except Exception:
                    # Игнорируем ошибки при закрытии
                    pass
                finally:
                    self._session = None
    
    @property
    def session(self) -> Optional[aiohttp.ClientSession]:
        """Получить текущую сессию"""
        return self._session
    
    async def recreate_session(self):
        """Пересоздать сессию"""
        await self.close()
        await asyncio.sleep(1.0)  # Даём время полностью закрыться
        await self.ensure_session()

