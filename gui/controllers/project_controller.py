"""
Контроллер управления проектами
"""

from pathlib import Path
from typing import Optional, Dict, Any

from seo_analyzer.core.query_groups import QueryGroupManager
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase


class ProjectController:
    """Контроллер управления проектами"""
    
    def __init__(self):
        self.group_manager = QueryGroupManager()
        self.master_db = MasterQueryDatabase()
    
    def get_group_stats(self, group_name: str) -> Optional[Dict[str, Any]]:
        """
        Получить статистику группы
        
        Args:
            group_name: Название группы
            
        Returns:
            Словарь со статистикой или None
        """
        try:
            df = self.master_db.load_queries(group_name, include_serp_urls=False)
            if df is None or df.empty:
                return None
            
            stats = {
                'total_queries': len(df),
                'avg_frequency': df['frequency_world'].mean() if 'frequency_world' in df.columns else 0,
            }
            
            # Количество кластеров
            if 'semantic_cluster_id' in df.columns:
                clusters = df['semantic_cluster_id'].dropna().unique()
                stats['clusters_count'] = len(clusters)
            else:
                stats['clusters_count'] = 0
            
            return stats
            
        except Exception as e:
            print(f"Ошибка получения статистики: {e}")
            return None

