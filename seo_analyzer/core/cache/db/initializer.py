"""
Инициализация базы данных Master Query
"""

import sqlite3
from pathlib import Path
from typing import Union

from ..master_query_schema import (
    MASTER_QUERY_TABLE_SCHEMA,
    MASTER_QUERY_INDEXES,
    MASTER_EXPORT_VIEW
)


def apply_sqlite_optimizations(cursor: sqlite3.Cursor):
    """
    Применяет PRAGMA оптимизации для ускорения работы SQLite
    
    Используется при каждом подключении к БД для максимальной производительности.
    Особенно важно для операций чтения (проверка кэша).
    
    Args:
        cursor: Курсор SQLite
    """
    # WAL режим - параллельные чтения во время записи
    cursor.execute("PRAGMA journal_mode = WAL")
    
    # Баланс скорости/безопасности
    cursor.execute("PRAGMA synchronous = NORMAL")
    
    # Увеличенный кэш - больше данных в RAM = быстрее чтение
    cursor.execute("PRAGMA cache_size = -2048000")  # 2GB (было 256MB)
    
    # Temp таблицы в памяти
    cursor.execute("PRAGMA temp_store = MEMORY")
    
    # Memory-mapped I/O - очень быстро для чтения больших БД
    cursor.execute("PRAGMA mmap_size = 2147483648")  # 2GB (было 256MB)
    
    # Таймаут для параллельных запросов
    cursor.execute("PRAGMA busy_timeout = 30000")  # 30 секунд
    
    # Автоочистка
    cursor.execute("PRAGMA auto_vacuum = INCREMENTAL")
    
    # Foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Автооптимизация запросов
    cursor.execute("PRAGMA optimize")


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
        apply_sqlite_optimizations(cursor)
        
        # Page size работает только при создании новой БД
        cursor.execute("PRAGMA page_size = 32768")  # 32KB
        
        # Выводим сообщения только при первом создании БД (проверяем существование таблиц)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='master_queries'")
        is_new_db = cursor.fetchone() is None
        
        if is_new_db:
            print("✓ SQLite оптимизации применены (WAL, cache 2GB, page 32KB, mmap 2GB)")
        
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
        
        # Выводим сообщение только при первом создании БД
        if is_new_db:
            print("✓ Master Query Database инициализирована")

