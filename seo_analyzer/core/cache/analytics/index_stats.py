"""
Статистика использования индексов
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any


class IndexStatistics:
    """Статистика использования индексов"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def get_index_usage_stats(self) -> List[Dict[str, Any]]:
        """
        Статистика использования индексов
        Показывает какие индексы реально используются
        
        Returns:
            Список индексов с информацией
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                name,
                tbl_name,
                sql
            FROM sqlite_master 
            WHERE type='index' AND name LIKE 'idx_master_%'
            ORDER BY name
        """)
        
        indexes = []
        for row in cursor.fetchall():
            indexes.append({
                'index_name': row[0],
                'table_name': row[1],
                'definition': row[2]
            })
        
        conn.close()
        
        return indexes

