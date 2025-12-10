"""
Master Query Database
Единая БД со ВСЕМИ данными по запросам для мгновенной переклассификации
"""

from pathlib import Path

from .db.initializer import DatabaseInitializer
from .db.optimizer import DatabaseOptimizer
from .operations.query_saver import QuerySaver
from .operations.query_loader import QueryLoader
from .operations.group_manager import GroupManager
from .operations.intent_updater import IntentUpdater
from .serp.status_manager import SERPStatusManager
from .serp.statistics import SERPStatistics
from .analytics.statistics import DatabaseStatistics
from .analytics.index_stats import IndexStatistics


class MasterQueryDatabase:
    """
    Master Database - единое хранилище ВСЕХ данных по запросам
    
    Преимущества:
    - Всё в одном месте: интент + SERP + метрики + бренды + воронка
    - Мгновенная переклассификация при изменении параметров кластеризации
    - Быстрый экспорт без JOIN'ов
    - Единый источник правды (Single Source of Truth)
    """
    
    def __init__(self, db_path: Path = None):
        """
        Args:
            db_path: Путь к БД (по умолчанию output/master_queries.db)
        """
        if db_path is None:
            db_path = Path("output/master_queries.db")
        
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Инициализация модулей
        self.initializer = DatabaseInitializer(self.db_path)
        self.optimizer = DatabaseOptimizer(self.db_path)
        self.query_loader = QueryLoader(self.db_path)
        self.query_saver = QuerySaver(self.db_path, self.query_loader)
        self.group_manager = GroupManager(self.db_path)
        self.intent_updater = IntentUpdater(self.db_path)
        self.serp_status = SERPStatusManager(self.db_path)
        self.serp_stats = SERPStatistics(self.db_path)
        self.db_stats = DatabaseStatistics(self.db_path)
        self.index_stats = IndexStatistics(self.db_path)
        
        # Инициализация БД
        self._init_database()
    
    def _init_database(self):
        """Инициализация таблиц БД с оптимизациями"""
        self.initializer.initialize()
    
    def save_queries(self, group_name: str, df, csv_path: Path = None, csv_hash: str = None):
        """Сохраняет/обновляет запросы в master таблице"""
        self.query_saver.save_queries(group_name, df, csv_path, csv_hash)
    
    def load_queries(self, group_name: str, include_serp_urls: bool = True):
        """Загружает ВСЕ данные по запросам из мастер-таблицы"""
        return self.query_loader.load_queries(group_name, include_serp_urls)
    
    def group_exists(self, group_name: str) -> bool:
        """Проверяет существует ли группа"""
        return self.group_manager.group_exists(group_name)
    
    def get_group_info(self, group_name: str):
        """Получает информацию о группе из таблицы query_groups"""
        return self.group_manager.get_group_info(group_name)
    
    def get_statistics(self, group_name: str = None):
        """Статистика по группе или всей БД"""
        return self.db_stats.get_statistics(group_name)
    
    def analyze_query_performance(self, query: str):
        """Анализирует производительность SQL запроса"""
        return self.db_stats.analyze_query_performance(query)
    
    def rebuild_indexes(self):
        """Пересоздаёт индексы"""
        self.optimizer.rebuild_indexes()
    
    def get_index_usage_stats(self):
        """Статистика использования индексов"""
        return self.index_stats.get_index_usage_stats()
    
    def optimize_database(self):
        """Полная оптимизация БД"""
        self.optimizer.optimize_database()
    
    def get_pending_serp_queries(self, group_name: str):
        """Получить запросы с незавершённой загрузкой SERP"""
        return self.serp_stats.get_pending_serp_queries(group_name)
    
    def update_serp_status(
        self,
        group_name: str,
        keyword: str,
        status: str,
        req_id: str = None,
        error_message: str = None
    ):
        """Обновить статус SERP запроса"""
        self.serp_status.update_serp_status(group_name, keyword, status, req_id, error_message)
    
    def mark_serp_as_pending(self, group_name: str, keywords):
        """Отметить запросы как ожидающие загрузки SERP"""
        self.serp_status.mark_serp_as_pending(group_name, keywords)
    
    def get_serp_statistics(self, group_name: str):
        """Статистика по SERP загрузке"""
        return self.serp_stats.get_serp_statistics(group_name)
    
    def update_serp_metrics(
        self,
        group_name: str,
        keyword: str,
        metrics,
        documents,
        lsi_phrases
    ):
        """Обновить SERP метрики для конкретного запроса"""
        self.serp_status.update_serp_metrics(group_name, keyword, metrics, documents, lsi_phrases)
    
    def update_intent(
        self,
        group_name: str,
        keyword: str,
        main_intent: str,
        commercial_score: float = None,
        informational_score: float = None
    ):
        """Обновить интент для конкретного запроса"""
        return self.intent_updater.update_intent(
            group_name, keyword, main_intent, commercial_score, informational_score
        )
    
    def update_intents_batch(self, group_name: str, updates):
        """Пакетное обновление интентов"""
        return self.intent_updater.update_intents_batch(group_name, updates)
    
    def update_intents_from_dataframe(self, group_name: str, df):
        """Обновить интенты из DataFrame"""
        return self.intent_updater.update_intents_from_dataframe(group_name, df)


__all__ = ['MasterQueryDatabase']
