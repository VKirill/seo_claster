"""
Управление группами запросов
"""

import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any


class GroupManager:
    """Менеджер групп запросов"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def group_exists(self, group_name: str) -> bool:
        """
        Проверяет существует ли группа
        
        Args:
            group_name: Название группы
            
        Returns:
            True если группа существует
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            'SELECT COUNT(*) FROM master_queries WHERE group_name = ?',
            (group_name,)
        )
        exists = cursor.fetchone()[0] > 0
        
        conn.close()
        return exists
    
    def get_group_info(self, group_name: str) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о группе из таблицы query_groups
        
        Args:
            group_name: Название группы
            
        Returns:
            Словарь с информацией о группе или None если группа не найдена
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT csv_file_path, csv_hash, total_queries, unique_queries, 
                   duplicates_removed, imported_at, updated_at
            FROM query_groups
            WHERE group_name = ?
        ''', (group_name,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result is None:
            return None
        
        return {
            'csv_file_path': result[0],
            'csv_hash': result[1],
            'total_queries': result[2],
            'unique_queries': result[3],
            'duplicates_removed': result[4],
            'imported_at': result[5],
            'updated_at': result[6]
        }

