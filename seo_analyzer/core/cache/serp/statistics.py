"""
Статистика по SERP данным
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Any


class SERPStatistics:
    """Статистика по SERP данным"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def get_serp_statistics(self, group_name: str) -> Dict[str, Any]:
        """
        Статистика по SERP загрузке
        
        Args:
            group_name: Название группы
            
        Returns:
            Статистика по статусам
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN serp_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN serp_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN serp_status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN serp_status = 'error' THEN 1 ELSE 0 END) as error,
                SUM(CASE WHEN serp_found_docs IS NOT NULL THEN 1 ELSE 0 END) as with_data
            FROM master_queries
            WHERE group_name = ?
        ''', (group_name,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row or row[0] == 0:
            return {
                'total': 0,
                'completed': 0,
                'pending': 0,
                'processing': 0,
                'error': 0,
                'with_data': 0,
                'completion_rate': 0.0
            }
        
        return {
            'total': row[0],
            'completed': row[1] or 0,
            'pending': row[2] or 0,
            'processing': row[3] or 0,
            'error': row[4] or 0,
            'with_data': row[5] or 0,
            'completion_rate': (row[1] or 0) / row[0] if row[0] > 0 else 0.0
        }
    
    def get_pending_serp_queries(self, group_name: str) -> List[Dict[str, Any]]:
        """
        Получить запросы с незавершённой загрузкой SERP
        
        Args:
            group_name: Название группы
            
        Returns:
            Список запросов со статусом pending/processing
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT keyword, serp_req_id, serp_status, serp_error_message
            FROM master_queries
            WHERE group_name = ? 
              AND serp_status IN ('pending', 'processing')
            ORDER BY created_at
        ''', (group_name,))
        
        pending = []
        for row in cursor.fetchall():
            pending.append({
                'keyword': row[0],
                'serp_req_id': row[1],
                'serp_status': row[2],
                'serp_error_message': row[3]
            })
        
        conn.close()
        return pending

