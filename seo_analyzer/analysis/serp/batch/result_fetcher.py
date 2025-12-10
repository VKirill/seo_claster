"""
Получение результатов по req_id из xmlstock API
"""

import asyncio
import aiohttp
import re
from typing import Dict, Any, Optional, Callable

from ..models.pending_request import PendingRequest
from .rate_limiter import RateLimiter
from .session_manager import SessionManager


class ResultFetcher:
    """Получатель результатов по req_id"""
    
    def __init__(
        self,
        user: str,
        key: str,
        url: str,
        rate_limiter: RateLimiter,
        session_manager: SessionManager
    ):
        """
        Args:
            user: Пользователь API
            key: Ключ API
            url: URL API
            rate_limiter: Rate limiter
            session_manager: Менеджер сессии
        """
        self.user = user
        self.key = key
        self.url = url
        self.rate_limiter = rate_limiter
        self.session_manager = session_manager
    
    async def fetch_result_by_req_id(
        self,
        pending: PendingRequest,
        semaphore: asyncio.Semaphore,
        progress_callback: Optional[Callable]
    ) -> Dict[str, Any]:
        """Получить результат по req_id"""
        
        async with semaphore:
            # Проверяем, был ли недавно 503 от других запросов
            await self.rate_limiter.check_and_wait_for_503()
            
            # Rate limiting: соблюдаем лимит запросов в секунду
            await self.rate_limiter.wait_for_rate_limit()
            
            # Убеждаемся что сессия активна
            await self.session_manager.ensure_session()
            session = self.session_manager.session
            
            # Проверяем что сессия не была закрыта между вызовами
            if session is None or session.closed:
                raise aiohttp.ClientConnectionError("Session is closed or None")
            
            params = {
                'user': self.user,
                'key': self.key,
                'req_id': pending.req_id
            }
            
            try:
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
                            
                            if '<html' in error_text.lower() or '<title>' in error_text.lower():
                                title_match = re.search(r'<title>([^<]+)</title>', error_text, re.IGNORECASE)
                                title = title_match.group(1) if title_match else "Service Temporarily Unavailable"
                                # Возвращаем специальный статус для retry с задержкой
                                return {
                                    'query': pending.query,
                                    'req_id': pending.req_id,
                                    'status': 'retry_503',
                                    'error': f"HTTP {response.status} {title} (сервер временно недоступен)",
                                    'xml_response': error_text
                                }
                            else:
                                # 503 но не HTML - тоже retry
                                return {
                                    'query': pending.query,
                                    'req_id': pending.req_id,
                                    'status': 'retry_503',
                                    'error': f"HTTP {response.status}: {error_text[:200]}",
                                    'xml_response': error_text
                                }
                        return {
                            'query': pending.query,
                            'req_id': pending.req_id,
                            'status': 'error',
                            'error': f"HTTP {response.status}: {error_text[:200]}",
                            'xml_response': error_text
                        }
                    
                    xml_text = await response.text()
                    
                    # Проверяем что ответ не HTML (может быть ошибка сервера)
                    if xml_text.strip().lower().startswith('<html'):
                        title_match = re.search(r'<title>([^<]+)</title>', xml_text, re.IGNORECASE)
                        title = title_match.group(1) if title_match else "Server Error"
                        h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', xml_text, re.IGNORECASE)
                        h1 = h1_match.group(1) if h1_match else ""
                        error_msg = f"{title}" + (f": {h1}" if h1 else "")
                        return {
                            'query': pending.query,
                            'req_id': pending.req_id,
                            'status': 'error',
                            'error': f"Сервер вернул HTML вместо XML: {error_msg}",
                            'xml_response': xml_text
                        }
                    
                    # Проверяем на 202 (ещё не готово)
                    if 'code="202"' in xml_text or 'не обработан' in xml_text:
                        return {
                            'query': pending.query,
                            'req_id': pending.req_id,
                            'status': 'pending',
                            'message': 'Result not ready yet (202)'
                        }
                    
                    # Проверяем на ошибки
                    if '<error' in xml_text:
                        error_match = re.search(r'<error[^>]*>([^<]+)</error>', xml_text)
                        error_msg = error_match.group(1) if error_match else xml_text[:200]
                        return {
                            'query': pending.query,
                            'req_id': pending.req_id,
                            'status': 'error',
                            'error': error_msg,
                            'xml_response': xml_text
                        }
                    
                    # Успешный ответ
                    return {
                        'query': pending.query,
                        'req_id': pending.req_id,
                        'status': 'completed',
                        'xml_response': xml_text
                    }
            
            except (aiohttp.ClientError, aiohttp.ClientConnectionError, ConnectionResetError, OSError) as e:
                # Сетевые ошибки - пересоздаём сессию и возвращаем pending для повтора
                try:
                    await self.session_manager.recreate_session()
                except Exception:
                    # Если не можем пересоздать - не критично, попробуем в следующий раз
                    pass
                
                # Возвращаем pending для повтора (вместо error)
                return {
                    'query': pending.query,
                    'req_id': pending.req_id,
                    'status': 'pending',
                    'message': f"Connection error (will retry): {type(e).__name__} - {str(e)}"
                }
            except asyncio.TimeoutError:
                # Таймауты - тоже повторяем
                return {
                    'query': pending.query,
                    'req_id': pending.req_id,
                    'status': 'pending',
                    'message': 'Request timeout (will retry)'
                }
            except Exception as e:
                return {
                    'query': pending.query,
                    'req_id': pending.req_id,
                    'status': 'error',
                    'error': f"Fetch error: {type(e).__name__} - {str(e)}"
                }

