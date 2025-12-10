"""
Group Database Manager
Управление БД для отдельных групп + общая БД для классификации
"""

import sqlite3
from pathlib import Path


class GroupDatabaseManager:
    """Менеджер единой БД для всех групп запросов"""
    
    # Путь к единой БД для всех данных (master_queries.db)
    GLOBAL_DB_PATH = Path("output/master_queries.db")
    
    def __init__(self, query_group: str = None):
        """
        Инициализация менеджера БД
        
        Args:
            query_group: Название группы запросов (опционально)
        """
        self.query_group = query_group
        self.GLOBAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Инициализация единой БД (если еще не создана)
        self._init_global_database()
    
    def _init_global_database(self):
        """
        Инициализация единой БД для всех данных
        
        Структура в master_queries.db:
        1. master_queries - основные данные запросов
        2. query_groups - метаданные групп
        3. domain_stats - классификация доменов (для DomainClassifier)
        
        Примечание: SERP данные теперь хранятся в master_queries, а не в отдельных таблицах
        """
        self.GLOBAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.GLOBAL_DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Таблица domain_stats для DomainClassifier (совместимость)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS domain_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT NOT NULL UNIQUE,
                    total_documents INTEGER DEFAULT 0,
                    offer_info_count INTEGER DEFAULT 0,
                    offer_info_ratio REAL DEFAULT 0.0,
                    classification TEXT DEFAULT 'unknown',
                    confidence REAL DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain_classification ON domain_stats(classification)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain_offers ON domain_stats(offer_info_count)")
            
            conn.commit()

