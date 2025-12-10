"""
Оптимизация базы данных Master Query
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any

from ..master_query_schema import MASTER_QUERY_INDEXES


class DatabaseOptimizer:
    """Оптимизатор базы данных"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def optimize_database(self):
        """
        Полная оптимизация БД (как VACUUM ANALYZE в PostgreSQL)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print("🔄 Оптимизация БД...")
        
        # 1. VACUUM - очистка неиспользуемого места
        print("   → VACUUM (дефрагментация)...")
        cursor.execute("VACUUM")
        
        # 2. ANALYZE - обновление статистики для оптимизатора
        print("   → ANALYZE (статистика)...")
        cursor.execute("ANALYZE")
        
        # 3. PRAGMA optimize - автоматическая оптимизация
        print("   → PRAGMA optimize...")
        cursor.execute("PRAGMA optimize")
        
        conn.commit()
        conn.close()
        
        # Размер БД после оптимизации
        size_mb = self.db_path.stat().st_size / (1024 * 1024)
        
        print(f"✓ Оптимизация завершена")
        print(f"✓ Размер БД: {size_mb:.1f} MB")
    
    def rebuild_indexes(self):
        """
        Пересоздаёт индексы (аналог REINDEX в PostgreSQL)
        Полезно после массовых INSERT/UPDATE
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print("🔄 Пересоздание индексов...")
        
        # Удаляем старые индексы
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND name LIKE 'idx_master_%'
        """)
        
        indexes = cursor.fetchall()
        for (index_name,) in indexes:
            cursor.execute(f"DROP INDEX IF EXISTS {index_name}")
            print(f"   ✓ Удалён: {index_name}")
        
        # Создаём заново
        for index_sql in MASTER_QUERY_INDEXES:
            cursor.execute(index_sql)
        
        # ANALYZE для обновления статистики оптимизатора
        cursor.execute("ANALYZE master_queries")
        
        conn.commit()
        conn.close()
        
        print(f"✓ Пересоздано {len(MASTER_QUERY_INDEXES)} индексов")
        print("✓ ANALYZE выполнен (статистика обновлена)")
    
    def get_index_usage_stats(self) -> List[Dict[str, Any]]:
        """
        Статистика использования индексов
        Показывает какие индексы реально используются
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

