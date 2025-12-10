"""
Отправка отложенных запросов в xmlstock API
"""

import asyncio
import aiohttp
import re
from datetime import datetime
from typing import Optional, Callable

from ..models.pending_request import PendingRequest
from .rate_limiter import RateLimiter
from .session_manager import SessionManager


class RequestSender:
    """Отправитель запросов с delayed=1"""
    
    def __init__(
        self,
        user: str,
        key: str,
        lr: int,
        url: str,
        rate_limiter: RateLimiter,
        session_manager: SessionManager,
        device: str = 'desktop'
    ):
        """
        Args:
            user: Пользователь API
            key: Ключ API
            lr: Регион
            url: URL API
            rate_limiter: Rate limiter
            session_manager: Менеджер сессии
            device: Устройство (desktop, mobile, tablet, iphone, android)
        """
        self.user = user
        self.key = key
        self.lr = lr
        self.device = device
        self.url = url
        self.rate_limiter = rate_limiter
        self.session_manager = session_manager
    
    async def send_delayed_request(
        self,
        query: str,
        index: int,
        total: int,
        semaphore: asyncio.Semaphore,
        progress_callback: Optional[Callable],
        on_req_id_received: Optional[Callable]
    ) -> PendingRequest:
        """Отправить запрос с delayed=1 и получить req_id"""
        
        async with semaphore:
            params = {
                'user': self.user,
                'key': self.key,
                'query': query,
                'lr': self.lr,
                'device': self.device,
                'groupby': 'attr=d.mode=deep.groups-on-page=20.docs-in-group=1',
                'maxpassages': 2,
                'filter': 'moderate',
                'delayed': '1'
            }
            
            # Retry логика для ошибок 503 (перегрузка сервера) и сетевых ошибок
            max_retries = 3
            for retry_attempt in range(max_retries):
                # Проверяем, был ли недавно 503 от других запросов
                await self.rate_limiter.check_and_wait_for_503()
                
                # Rate limiting: соблюдаем лимит запросов в секунду
                await self.rate_limiter.wait_for_rate_limit()
                
                try:
                    # Убеждаемся что сессия активна
                    await self.session_manager.ensure_session()
                    session = self.session_manager.session
                    
                    # Проверяем что сессия не была закрыта между вызовами
                    if session is None or session.closed:
                        raise aiohttp.ClientConnectionError("Session is closed or None")
                    
                    async with session.get(
                        self.url,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        # Проверяем HTTP статус код
                        if response.status != 200:
                            error_text = await response.text()
                            # Пытаемся извлечь информацию об ошибке из HTML
                            if response.status == 503:
                                # Отмечаем глобально что сервер перегружен
                                await self.rate_limiter.mark_503_error()
                                
                                # Проверяем на HTML страницу с ошибкой
                                if '<html' in error_text.lower() or '<title>' in error_text.lower():
                                    title_match = re.search(r'<title>([^<]+)</title>', error_text, re.IGNORECASE)
                                    title = title_match.group(1) if title_match else "Service Temporarily Unavailable"
                                    
                                    # Если не последняя попытка - ждём 60 сек и повторяем
                                    if retry_attempt < max_retries - 1:
                                        print(f"   ⚠️  Сервер перегружен (503) для запроса '{query[:50]}...'. Ожидание 60 сек перед повтором...")
                                        await asyncio.sleep(60)
                                        continue  # Повторяем попытку
                                    
                                    raise Exception(f"HTTP {response.status} {title} (сервер временно недоступен после {max_retries} попыток)")
                                else:
                                    # 503 но не HTML - тоже ждём
                                    if retry_attempt < max_retries - 1:
                                        print(f"   ⚠️  Сервер перегружен (503) для запроса '{query[:50]}...'. Ожидание 60 сек перед повтором...")
                                        await asyncio.sleep(60)
                                        continue
                                    
                                    raise Exception(f"HTTP {response.status}: {error_text[:200]}")
                            raise Exception(f"HTTP {response.status}: {error_text[:200]}")
                        
                        xml_text = await response.text()
                        
                        # Проверяем что ответ не HTML (может быть ошибка сервера)
                        if xml_text.strip().lower().startswith('<html'):
                            # Пытаемся извлечь информацию об ошибке из HTML
                            title_match = re.search(r'<title>([^<]+)</title>', xml_text, re.IGNORECASE)
                            title = title_match.group(1) if title_match else "Server Error"
                            h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', xml_text, re.IGNORECASE)
                            h1 = h1_match.group(1) if h1_match else ""
                            error_msg = f"{title}" + (f": {h1}" if h1 else "")
                            raise Exception(f"Сервер вернул HTML вместо XML: {error_msg}")
                        
                        # Проверяем на ошибки API
                        if '<error' in xml_text:
                            error_match = re.search(r'<error[^>]*code="([^"]*)"[^>]*>([^<]+)</error>', xml_text)
                            if error_match:
                                error_code = error_match.group(1)
                                error_msg = error_match.group(2)
                                raise Exception(f"API error (code={error_code}): {error_msg}")
                            else:
                                # Пытаемся извлечь текст ошибки другим способом
                                error_match = re.search(r'<error[^>]*>([^<]+)</error>', xml_text)
                                error_msg = error_match.group(1) if error_match else xml_text[:200]
                                raise Exception(f"API error: {error_msg}")
                        
                        # Извлекаем req_id
                        req_id_match = re.search(r'<req_id>([^<]+)</req_id>', xml_text)
                        if not req_id_match:
                            raise Exception(f"No req_id in response (возможно ошибка API): {xml_text[:200]}")
                        
                        req_id = req_id_match.group(1)
                        
                        # Сохраняем req_id в БД СРАЗУ после получения
                        if on_req_id_received:
                            try:
                                on_req_id_received(query, req_id)
                            except Exception as e:
                                # Ошибка сохранения - логируем, но продолжаем отправку
                                print(f"   ⚠️  Ошибка в callback сохранения req_id для '{query[:50]}...': {e}")
                        
                        # Progress callback
                        if progress_callback and index % 50 == 0:
                            progress_callback(index, total, query, 'sent')
                        
                        return PendingRequest(
                            query=query,
                            req_id=req_id,
                            sent_at=datetime.now()
                        )
                
                except asyncio.TimeoutError:
                    if retry_attempt < max_retries - 1:
                        await asyncio.sleep(2)  # Короткая пауза перед повтором
                        continue
                    raise Exception(f"Timeout при отправке запроса (10 сек) после {max_retries} попыток")
                except (aiohttp.ClientError, aiohttp.ClientConnectionError, ConnectionResetError, OSError) as e:
                    # Сетевые ошибки - пересоздаём сессию
                    error_msg = f"{type(e).__name__} - {str(e)}"
                    
                    if retry_attempt < max_retries - 1:
                        wait_time = (retry_attempt + 1) * 2  # 2, 4, 6 сек
                        await self.session_manager.recreate_session()
                        await asyncio.sleep(wait_time)
                        continue
                    raise Exception(f"Сетевая ошибка после {max_retries} попыток: {error_msg}")
                except Exception as e:
                    # Если уже наше исключение с информацией - пробрасываем как есть
                    if "API error" in str(e) or "No req_id" in str(e):
                        if retry_attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        raise Exception(f"Запрос '{query}': {str(e)}")
                    raise Exception(f"Запрос '{query}': {type(e).__name__} - {str(e)}")

