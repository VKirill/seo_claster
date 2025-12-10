"""
Поиск незавершённых запросов в Master DB
"""

import sqlite3
from typing import List, Dict, Any


class PendingQueriesFinder:
    """Поиск незавершённых запросов"""
    
    @staticmethod
    def find_pending_queries(db_path: str) -> List[Dict[str, Any]]:
        """
        Найти все незавершённые запросы в Master DB
        
        Args:
            db_path: Путь к Master DB
            
        Returns:
            Список словарей с информацией о незавершённых запросах
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все группы с незавершёнными запросами
        cursor.execute('''
            SELECT DISTINCT group_name 
            FROM master_queries
            WHERE (
                (serp_status = 'processing' AND serp_req_id IS NOT NULL)
                OR serp_status = 'pending'
                OR serp_status = 'error'
                OR serp_status = 'failed'
                OR (serp_status = 'completed' AND (serp_found_docs IS NULL OR serp_found_docs = 0))
            )
            ORDER BY group_name
        ''')
        groups_with_pending = [row[0] for row in cursor.fetchall()]
        
        if not groups_with_pending:
            conn.close()
            return []
        
        # Собираем все незавершённые запросы
        all_pending = []
        for group_name in groups_with_pending:
            cursor.execute('''
                SELECT keyword, serp_req_id, group_name, serp_status
                FROM master_queries
                WHERE group_name = ? AND serp_status = 'processing' AND serp_req_id IS NOT NULL
                ORDER BY serp_updated_at
            ''', (group_name,))
            
            for row in cursor.fetchall():
                all_pending.append({
                    'query': row[0],
                    'req_id': row[1],
                    'group': row[2],
                    'status': row[3],
                    'needs_new_request': False
                })
        
        conn.close()
        return all_pending

