"""Миграции схемы базы данных SERP"""

import sqlite3


def init_database_schema(conn: sqlite3.Connection):
    """
    Инициализация таблиц БД
    
    Args:
        conn: SQLite соединение
    """
    cursor = conn.cursor()
    
    # Таблица с SERP результатами
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS serp_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            query_hash TEXT NOT NULL,
            query_group TEXT DEFAULT NULL,
            lr INTEGER NOT NULL,
            req_id TEXT,
            status TEXT DEFAULT 'completed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            xml_response TEXT NOT NULL,
            found_docs INTEGER,
            main_pages_count INTEGER,
            titles_with_keyword INTEGER,
            commercial_domains INTEGER,
            info_domains INTEGER,
            error_message TEXT,
            UNIQUE(query_hash, lr, query_group)
        )
    """)
    
    # Добавляем колонки req_id и status если их нет (для существующих БД)
    try:
        cursor.execute("ALTER TABLE serp_results ADD COLUMN req_id TEXT")
    except sqlite3.OperationalError:
        pass  # Колонка уже существует
    
    try:
        cursor.execute("ALTER TABLE serp_results ADD COLUMN status TEXT DEFAULT 'completed'")
    except sqlite3.OperationalError:
        pass  # Колонка уже существует
    
    # Таблица с извлеченными документами из SERP
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS serp_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            serp_result_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            url TEXT NOT NULL,
            domain TEXT,
            title TEXT,
            snippet TEXT,
            passages TEXT,
            is_commercial BOOLEAN,
            FOREIGN KEY (serp_result_id) REFERENCES serp_results(id) ON DELETE CASCADE
        )
    """)
    
    # Таблица с уникальными LSI фразами (нормализованная)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unique_lsi_phrases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phrase TEXT NOT NULL UNIQUE,
            total_frequency INTEGER DEFAULT 0
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_unique_phrase 
        ON unique_lsi_phrases(phrase)
    """)
    
    # Junction table для связи запросов с фразами
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS serp_lsi_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            serp_result_id INTEGER NOT NULL,
            phrase_id INTEGER NOT NULL,
            frequency INTEGER DEFAULT 1,
            source TEXT,
            FOREIGN KEY (serp_result_id) REFERENCES serp_results(id) ON DELETE CASCADE,
            FOREIGN KEY (phrase_id) REFERENCES unique_lsi_phrases(id) ON DELETE CASCADE,
            UNIQUE(serp_result_id, phrase_id)
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_serp_lsi_serp 
        ON serp_lsi_mapping(serp_result_id)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_serp_lsi_phrase 
        ON serp_lsi_mapping(phrase_id)
    """)
    
    # Индексы для быстрого поиска
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_query_hash 
        ON serp_results(query_hash, lr)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_query_group 
        ON serp_results(query_group)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_created_at 
        ON serp_results(created_at)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_serp_docs 
        ON serp_documents(serp_result_id)
    """)
    
    # Индекс для поиска по req_id
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_req_id 
        ON serp_results(req_id)
    """)
    
    # Индекс для поиска незавершённых запросов
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_status 
        ON serp_results(status)
    """)
    
    conn.commit()

