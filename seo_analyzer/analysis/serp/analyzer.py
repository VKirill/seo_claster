"""
SERP Analyzer
Анализатор SERP с кэшированием в SQLite и файловом кэше
"""

from typing import Optional, Dict, List, Any

from .api_client import SERPAPIClient
from .core.query_analyzer import QueryAnalyzer
from .core.batch_processor import BatchProcessor
from .core.result_formatter import ResultFormatter
from .core.master_db_handler import MasterDBHandler
from .core.recovery_handler import RecoveryHandler


class SERPAnalyzer:
    """Анализатор SERP через xmlstock API"""
    
    def __init__(
        self,
        api_key: str,
        lr: int = 213,
        max_retries: int = 5,
        retry_delay: int = 5,
        timeout: int = 10,
        query_group: str = None,
        max_concurrent: int = 50,
        use_master_db: bool = True,
        use_batch_async: bool = True,
        device: str = 'desktop',
        site: str = None
    ):
        """
        Инициализация анализатора SERP
        
        Args:
            api_key: API ключ xmlstock
            lr: Регион (213 = Москва)
            max_retries: Макс попыток при ошибках
            retry_delay: Задержка между попытками
            timeout: Таймаут запроса
            query_group: Название группы запросов
            max_concurrent: Максимум параллельных запросов (глобально для всех групп)
            use_master_db: Использовать Master DB (единственный источник данных)
            use_batch_async: Массовый асинхронный режим (отправка всех сразу, потом получение)
            device: Устройство (desktop, mobile, tablet, iphone, android)
            site: Домен для фильтрации (site:domain.ru)
        """
        self.api_key = api_key
        self.lr = lr
        self.device = device
        self.site = site
        self.query_group = query_group
        self.use_master_db = use_master_db
        self.use_batch_async = use_batch_async
        
        # Master DB - ЕДИНСТВЕННЫЙ источник SERP данных
        self.master_db = None
        if use_master_db:
            try:
                from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
                self.master_db = MasterQueryDatabase()
            except Exception as e:
                print(f"⚠️  Master DB недоступен: {e}")
                self.master_db = None
        
        # API клиент с глобальным семафором
        self.api_client = SERPAPIClient(
            api_key=api_key,
            lr=lr,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            max_concurrent=max_concurrent,
            device=device,
            site=site
        )
        
        # Статистика
        self.stats = {
            'total_queries': 0,
            'cached_from_master': 0,
            'api_requests': 0,
            'errors': 0,
            'status_updated': 0,
        }
        
        # Инициализация модулей
        result_formatter = ResultFormatter(lr)
        master_db_handler = MasterDBHandler(self.master_db, query_group, lr)
        recovery_handler = RecoveryHandler(api_key, lr, master_db_handler, query_group)
        
        self.query_analyzer = QueryAnalyzer(
            self.api_client,
            master_db_handler,
            result_formatter,
            self.stats,
            lr
        )
        
        self.batch_processor = BatchProcessor(
            api_key,
            lr,
            master_db_handler,
            result_formatter,
            self.stats,
            recovery_handler,
            device=device,
            site=site
        )
        
        self.recovery_handler = recovery_handler
    
    async def analyze_query(
        self,
        query: str,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Анализировать один запрос с отслеживанием статуса"""
        return await self.query_analyzer.analyze_query(query, force_refresh)
    
    async def analyze_queries_batch(
        self,
        queries: List[str],
        max_concurrent: int = None,
        progress_callback: Optional[callable] = None,
        batch_size: int = 500
    ) -> List[Dict[str, Any]]:
        """Анализировать пакет запросов с батчингом"""
        return await self.query_analyzer.analyze_queries_batch(
            queries,
            max_concurrent,
            progress_callback,
            batch_size,
            self.use_batch_async,
            self.batch_processor
        )
    
    async def recover_pending_requests(self) -> int:
        """Автоматическое восстановление незавершённых запросов из всех групп"""
        return await self.recovery_handler.recover_pending_requests()
    
    async def recover_missing_lsi_from_urls(self, group_name: str = None) -> int:
        """Дособрать LSI фразы для запросов, у которых есть URL, но нет LSI"""
        return await self.recovery_handler.recover_missing_lsi_from_urls(group_name)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику работы анализатора"""
        return {
            'total_queries': self.stats['total_queries'],
            'cached_from_master': self.stats['cached_from_master'],
            'api_requests': self.stats['api_requests'],
            'errors': self.stats['errors'],
            'cache_hit_rate': (
                self.stats['cached_from_master'] / self.stats['total_queries'] * 100
                if self.stats['total_queries'] > 0 else 0
            )
        }
    
    async def close(self):
        """Закрыть соединения и освободить ресурсы"""
        await self.api_client.close()
    
    def clear_caches(self, clear_database: bool = False):
        """Очистить кэши (устаревший метод, оставлен для совместимости)"""
        if clear_database:
            print("⚠️  Очистка Master DB не поддерживается через этот метод")
        pass
