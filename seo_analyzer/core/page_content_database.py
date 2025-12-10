"""База данных для хранения контента страниц"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime


class PageContentDatabase:
    """БД для хранения breadcrumbs и контента страниц"""
    
    def __init__(self, db_path: Path):
        """
        Инициализация БД
        
        Args:
            db_path: Путь к файлу БД
        """
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Инициализация таблиц"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Таблица для контента страниц
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS page_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                query TEXT,
                position INTEGER,
                title TEXT,
                h1 TEXT,
                meta_description TEXT,
                breadcrumbs TEXT,
                text_content TEXT,
                text_length INTEGER,
                words_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Индексы для быстрого поиска
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_url ON page_content(url)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_query ON page_content(query)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_domain ON page_content(domain)
        ''')
        
        conn.commit()
        conn.close()
    
    def save_page_data(
        self,
        url: str,
        domain: str,
        query: str,
        position: int,
        content_data: Dict,
        breadcrumbs: Optional[List[str]] = None
    ):
        """
        Сохранить данные страницы
        
        Args:
            url: URL страницы
            domain: Домен
            query: Поисковый запрос
            position: Позиция в SERP
            content_data: Данные контента (из PageContentExtractor)
            breadcrumbs: Breadcrumbs
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        breadcrumbs_json = json.dumps(breadcrumbs, ensure_ascii=False) if breadcrumbs else None
        
        cursor.execute('''
            INSERT OR REPLACE INTO page_content
            (url, domain, query, position, title, h1, meta_description, 
             breadcrumbs, text_content, text_length, words_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            url,
            domain,
            query,
            position,
            content_data.get('title', ''),
            content_data.get('h1', ''),
            content_data.get('meta_description', ''),
            breadcrumbs_json,
            content_data.get('text', ''),
            content_data.get('text_length', 0),
            content_data.get('words_count', 0),
            datetime.now()
        ))
        
        conn.commit()
        conn.close()
    
    def get_page_data(self, url: str) -> Optional[Dict]:
        """
        Получить данные страницы
        
        Args:
            url: URL страницы
            
        Returns:
            Словарь с данными или None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT url, domain, query, position, title, h1, meta_description,
                   breadcrumbs, text_length, words_count, created_at
            FROM page_content
            WHERE url = ?
        ''', (url,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            breadcrumbs = json.loads(row[7]) if row[7] else None
            
            return {
                'url': row[0],
                'domain': row[1],
                'query': row[2],
                'position': row[3],
                'title': row[4],
                'h1': row[5],
                'meta_description': row[6],
                'breadcrumbs': breadcrumbs,
                'text_length': row[8],
                'words_count': row[9],
                'created_at': row[10]
            }
        
        return None
    
    def get_pages_by_query(self, query: str) -> List[Dict]:
        """Получить все страницы для запроса"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT url, domain, position, title, breadcrumbs, text_length, words_count
            FROM page_content
            WHERE query = ?
            ORDER BY position
        ''', (query,))
        
        rows = cursor.fetchall()
        conn.close()
        
        results = []
        for row in rows:
            breadcrumbs = json.loads(row[4]) if row[4] else None
            
            results.append({
                'url': row[0],
                'domain': row[1],
                'position': row[2],
                'title': row[3],
                'breadcrumbs': breadcrumbs,
                'text_length': row[5],
                'words_count': row[6]
            })
        
        return results


