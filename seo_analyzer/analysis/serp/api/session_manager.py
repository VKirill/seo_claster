"""
Управление aiohttp сессией
"""

import asyncio
import aiohttp
from typing import Optional


class SessionManager:
    """Менеджер сессии aiohttp"""
    
    def __init__(self):
        """Инициализация менеджера сессии"""
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def ensure_session(self):
        """Создать сессию если её нет"""
        if self._session is None or self._session.closed:
            # Закрываем старую сессию если есть (на всякий случай)
            if self._session is not None:
                try:
                    if not self._session.closed:
                        await self._session.close()
                    # Закрываем connector если есть
                    if hasattr(self._session, '_connector') and self._session._connector:
                        await self._session._connector.close()
                    await asyncio.sleep(0.1)
                except Exception:
                    # Игнорируем ошибки при закрытии
                    pass
                finally:
                    self._session = None
            self._session = aiohttp.ClientSession()
    
    async def close(self):
        """Закрыть сессию"""
        if self._session:
            try:
                if not self._session.closed:
                    await self._session.close()
                # Закрываем connector если есть
                if hasattr(self._session, '_connector') and self._session._connector:
                    await self._session._connector.close()
                await asyncio.sleep(0.1)
            except Exception:
                # Игнорируем ошибки при закрытии
                pass
            finally:
                self._session = None
    
    @property
    def session(self) -> Optional[aiohttp.ClientSession]:
        """Получить текущую сессию"""
        return self._session

