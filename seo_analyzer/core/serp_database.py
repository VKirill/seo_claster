"""
SERP Database Module (фасад для обратной совместимости)
Хранит все данные из xmlstock в SQLite для быстрого доступа без повторных запросов к API

⚠️ DEPRECATED: Этот модуль устарел. SERPDatabase удалён.
Все данные теперь хранятся в master_queries.db (MasterQueryDatabase).
Используйте MasterQueryDatabase для работы с данными.
"""

from pathlib import Path
from typing import Optional, Dict, List, Any

# SERPDatabase удалён - используйте MasterQueryDatabase
# from .serp import SERPDatabase as SERPDatabaseImpl

raise ImportError(
    "SERPDatabase устарел и удалён. "
    "Используйте MasterQueryDatabase из seo_analyzer.core.cache.master_query_db"
)


class SERPDatabase:
    """
    SQLite база данных для хранения SERP данных из xmlstock
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.core.serp
    """
    
    def __init__(self, db_path: Path = None):
        self._db = SERPDatabaseImpl(db_path)
        # Делегируем все атрибуты для обратной совместимости
        self.db_path = self._db.db_path
    
    def get_serp_data(self, query: str, lr: int = 213, max_age_days: int = 30) -> Optional[Dict[str, Any]]:
        """Получить SERP данные из БД"""
        return self._db.get_serp_data(query, lr, max_age_days)
    
    def save_serp_data(self, query: str, lr: int, xml_response: str, metrics: Dict[str, int], documents: List[Dict], lsi_phrases: List[Dict], error_message: Optional[str] = None) -> int:
        """Сохранить SERP данные в БД"""
        return self._db.save_serp_data(query, lr, xml_response, metrics, documents, lsi_phrases, error_message)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику по БД"""
        return self._db.get_statistics()
    
    def clear_old_data(self, days: int = 30) -> int:
        """Удалить данные старше указанного количества дней"""
        return self._db.clear_old_data(days)
    
    def get_all_queries(self, limit: int = None) -> List[Dict[str, Any]]:
        """Получить список всех сохраненных запросов"""
        return self._db.get_all_queries(limit)
    
    def query_exists(self, query: str, lr: int = 213, max_age_days: int = 30) -> bool:
        """Проверить существование актуальных данных для запроса"""
        return self._db.query_exists(query, lr, max_age_days)
    
    def export_to_json(self, output_path: Path):
        """Экспорт всей БД в JSON для бэкапа"""
        return self._db.export_to_json(output_path)

