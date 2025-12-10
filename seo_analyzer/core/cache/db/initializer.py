"""
Инициализация базы данных Master Query
"""

import sqlite3
from pathlib import Path

from ..master_query_schema import (
    MASTER_QUERY_TABLE_SCHEMA,
    MASTER_QUERY_INDEXES,
    MASTER_EXPORT_VIEW
)


class DatabaseInitializer:
    """Инициализатор базы данных"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def initialize(self):
        """
        Инициализация таблиц БД с оптимизациями (как PostgreSQL)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # PRAGMA оптимизации (аналог PostgreSQL настроек)
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA cache_size = -64000")  # 64MB
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA page_size = 32768")
        cursor.execute("PRAGMA auto_vacuum = INCREMENTAL")
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA optimize")
        
        print("✓ SQLite оптимизации применены (WAL, cache 64MB, page 32KB)")
        
        # Создаём таблицу групп
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS query_groups (
                group_name TEXT PRIMARY KEY,
                csv_file_path TEXT NOT NULL,
                csv_hash TEXT NOT NULL,
                total_queries INTEGER NOT NULL,
                unique_queries INTEGER NOT NULL,
                duplicates_removed INTEGER NOT NULL,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processing_version TEXT DEFAULT '2.0'
            )
        ''')
        
        # Создаём master таблицу
        cursor.execute(MASTER_QUERY_TABLE_SCHEMA)
        
        # Создаём индексы
        for index_sql in MASTER_QUERY_INDEXES:
            cursor.execute(index_sql)
        
        # Создаём view для экспорта
        cursor.execute(MASTER_EXPORT_VIEW)
        
        conn.commit()
        conn.close()
        
        print("✓ Master Query Database инициализирована")

