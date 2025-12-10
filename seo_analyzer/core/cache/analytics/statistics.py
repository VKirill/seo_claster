"""
Общая статистика по Master DB
"""

import sqlite3
from pathlib import Path
from typing import Dict, Any


class DatabaseStatistics:
    """Статистика по базе данных"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def get_statistics(self, group_name: str = None) -> Dict[str, Any]:
        """
        Статистика по группе или всей БД
        
        Args:
            group_name: Название группы (если None - по всей БД)
            
        Returns:
            Словарь со статистикой
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if group_name:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN main_intent IS NOT NULL THEN 1 ELSE 0 END) as with_intent,
                    SUM(CASE WHEN serp_found_docs IS NOT NULL THEN 1 ELSE 0 END) as with_serp,
                    SUM(CASE WHEN direct_shows IS NOT NULL THEN 1 ELSE 0 END) as with_direct,
                    AVG(kei) as avg_kei,
                    AVG(serp_offer_ratio) as avg_offer_ratio
                FROM master_queries
                WHERE group_name = ?
            ''', (group_name,))
        else:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN main_intent IS NOT NULL THEN 1 ELSE 0 END) as with_intent,
                    SUM(CASE WHEN serp_found_docs IS NOT NULL THEN 1 ELSE 0 END) as with_serp,
                    SUM(CASE WHEN direct_shows IS NOT NULL THEN 1 ELSE 0 END) as with_direct,
                    AVG(kei) as avg_kei,
                    AVG(serp_offer_ratio) as avg_offer_ratio
                FROM master_queries
            ''')
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            'total_queries': result[0],
            'with_intent': result[1],
            'with_serp': result[2],
            'with_direct': result[3],
            'avg_kei': round(result[4], 2) if result[4] else 0,
            'avg_offer_ratio': round(result[5], 4) if result[5] else 0,
        }
    
    def analyze_query_performance(self, query: str) -> Dict[str, Any]:
        """
        Анализирует производительность SQL запроса (EXPLAIN QUERY PLAN)
        Аналог EXPLAIN ANALYZE в PostgreSQL
        
        Args:
            query: SQL запрос для анализа
            
        Returns:
            План выполнения запроса
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(f"EXPLAIN QUERY PLAN {query}")
        plan = cursor.fetchall()
        
        conn.close()
        
        return {
            'query': query,
            'execution_plan': [
                {
                    'id': row[0],
                    'parent': row[1],
                    'detail': row[3]
                }
                for row in plan
            ]
        }

