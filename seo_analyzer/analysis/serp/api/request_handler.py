"""
Обработка HTTP запросов к API
"""

import asyncio
import aiohttp
from typing import Dict, Any

from .error_handler import ErrorHandler
from .response_parser import ResponseParser
from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor


class RequestHandler:
    """Обработчик HTTP запросов"""
    
    def __init__(
        self,
        lr: int,
        timeout: int,
        enricher: SERPDataEnricher,
        lsi_extractor: LSIExtractor,
        session: aiohttp.ClientSession,
        device: str = 'desktop'
    ):
        """
        Args:
            lr: Регион поиска
            timeout: Таймаут запроса
            enricher: Обогатитель SERP данных
            lsi_extractor: Извлекатель LSI фраз
            session: aiohttp сессия
            device: Устройство (desktop, mobile, tablet, iphone, android)
        """
        self.lr = lr
        self.device = device
        self.timeout = timeout
        self.enricher = enricher
        self.lsi_extractor = lsi_extractor
        self.session = session
        self.error_handler = ErrorHandler()
        self.response_parser = ResponseParser()
    
    async def fetch_async_mode(
        self,
        url: str,
        user: str,
        key: str,
        query: str
    ) -> Dict[str, Any]:
        """
        Асинхронный режим XMLStock: получаем req_id, затем результат
        
        Использует старую логику из backup файла для сохранения функциональности
        """
        import sys
        import importlib.util
        from pathlib import Path
        
        backup_path = Path(__file__).parent.parent / 'api_client.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("api_client_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.SERPAPIClient(
                api_key=f"{user}:{key}",
                lr=self.lr,
                timeout=self.timeout
            )
            temp_instance._session = self.session
            temp_instance.enricher = self.enricher
            temp_instance.lsi_extractor = self.lsi_extractor
            return await temp_instance._fetch_async_mode(url, user, key, query)
        else:
            return {
                'query': query,
                'lr': self.lr,
                'xml_response': None,
                'error': 'Backup file not found',
                'metrics': self.enricher._get_empty_metrics(),
                'documents': [],
                'lsi_phrases': []
            }
    
    async def fetch_hybrid_mode(
        self,
        url: str,
        params: Dict[str, Any],
        query: str,
        max_retries: int,
        retry_delay: int
    ) -> Dict[str, Any]:
        """
        Гибридный режим (текущая реализация)
        
        Использует старую логику из backup файла для сохранения функциональности
        """
        import sys
        import importlib.util
        from pathlib import Path
        
        backup_path = Path(__file__).parent.parent / 'api_client.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("api_client_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.SERPAPIClient(
                api_key=params.get('user', '') + ':' + params.get('key', ''),
                lr=self.lr,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=self.timeout
            )
            temp_instance._session = self.session
            temp_instance.enricher = self.enricher
            temp_instance.lsi_extractor = self.lsi_extractor
            return await temp_instance._fetch_hybrid_mode(url, params, query)
        else:
            return {
                'query': query,
                'lr': self.lr,
                'xml_response': None,
                'error': 'Backup file not found',
                'metrics': self.enricher._get_empty_metrics(),
                'documents': [],
                'lsi_phrases': []
            }

