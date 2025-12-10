"""
SERP API Client
Клиент для работы с xmlstock API с retry логикой
Фасад для модулей API
"""

import asyncio
from typing import Dict, Any

from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor
from .api_semaphore import get_api_semaphore
from .api.error_handler import ErrorHandler
from .api.session_manager import SessionManager
from .api.request_handler import RequestHandler


class SERPAPIClient:
    """Клиент для работы с xmlstock API"""
    
    def __init__(
        self,
        api_key: str,
        lr: int = 213,
        max_retries: int = 5,
        retry_delay: int = 5,
        timeout: int = 10,
        max_concurrent: int = 50,
        device: str = 'desktop',
        site: str = None
    ):
        """
        Args:
            api_key: API ключ xmlstock в формате "user:key"
            lr: Регион (213 = Москва)
            max_retries: Максимум попыток при ошибках
            retry_delay: Задержка между попытками (сек)
            timeout: Таймаут запроса (сек)
            max_concurrent: Максимум одновременных запросов (глобально)
            device: Устройство (desktop, mobile, tablet, iphone, android)
            site: Домен для фильтрации (site:domain.ru)
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.site = site
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        
        # Enricher и LSI
        self.enricher = SERPDataEnricher()
        self.lsi_extractor = LSIExtractor()
        
        # Глобальный семафор для контроля параллельности
        self.api_semaphore = get_api_semaphore(max_concurrent)
        
        # Инициализация модулей
        self.session_manager = SessionManager()
        self.error_handler = ErrorHandler()
    
    async def _ensure_session(self):
        """Создать сессию если её нет"""
        await self.session_manager.ensure_session()
    
    async def close(self):
        """Закрыть сессию"""
        await self.session_manager.close()
    
    async def fetch_serp_data(self, query: str) -> Dict[str, Any]:
        """
        Запрос к xmlstock API с retry логикой и глобальным контролем параллельности
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Dict с SERP метриками, документами и LSI фразами
        """
        # Убедимся что сессия создана
        await self._ensure_session()
        
        # Захватываем слот в глобальном семафоре
        async with self.api_semaphore:
            return await self._fetch_serp_data_internal(query)
    
    async def _fetch_serp_data_internal(self, query: str, use_async_mode: bool = True) -> Dict[str, Any]:
        """
        Внутренний метод для выполнения запроса к API
        
        Args:
            query: Поисковый запрос
            use_async_mode: Использовать асинхронный режим (delayed=1)
            
        Returns:
            Dict с SERP метриками, документами и LSI фразами
        """
        url = "https://xmlstock.com/yandex/xml/"
        
        # Парсинг API ключа
        if ':' in self.api_key:
            user, key = self.api_key.split(':', 1)
        else:
            user = self.api_key
            key = self.api_key
        
        # Добавляем site: к запросу если указан домен
        actual_query = query
        if self.site:
            actual_query = f"{query} site:{self.site}"
        
        # Создаем обработчик запросов
        request_handler = RequestHandler(
            lr=self.lr,
            device=self.device,
            timeout=self.timeout,
            enricher=self.enricher,
            lsi_extractor=self.lsi_extractor,
            session=self.session_manager.session
        )
        
        # Если асинхронный режим - используем async mode
        if use_async_mode:
            return await request_handler.fetch_async_mode(url, user, key, actual_query)
        
        # Гибридный режим (по умолчанию)
        params = {
            'user': user,
            'key': key,
            'query': actual_query,
            'lr': self.lr,
            'device': self.device,
            'groupby': 'attr=d.mode=deep.groups-on-page=20.docs-in-group=1',
            'maxpassages': 2,
            'filter': 'moderate'
        }
        
        return await request_handler.fetch_hybrid_mode(
            url, params, actual_query, self.max_retries, self.retry_delay
        )
    
    # Методы для обратной совместимости
    @classmethod
    async def _check_and_wait_for_503(cls):
        """Проверить, был ли недавно 503, и если да - подождать"""
        await ErrorHandler.check_and_wait_for_503()
    
    @classmethod
    async def _mark_503_error(cls):
        """Отметить что сервер вернул 503"""
        await ErrorHandler.mark_503_error()
