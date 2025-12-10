"""
Поиск похожих запросов по URL
"""

import sys
import importlib.util
from pathlib import Path
from typing import Dict, List


class SimilarityFinder:
    """Поиск похожих запросов"""
    
    @staticmethod
    def find_similar_queries_fast(
        query: str,
        query_urls: List[str],
        url_index: Dict[str, set],
        fast_similarity_calculator,
        top_positions: int = 30
    ) -> Dict[str, int]:
        """
        Быстрый поиск похожих запросов через инвертированный индекс
        
        Args:
            query: Запрос для поиска
            query_urls: URL запроса
            url_index: Инвертированный индекс URL → запросы
            fast_similarity_calculator: Калькулятор схожести
            top_positions: Глубина анализа
            
        Returns:
            Словарь запрос → количество общих URL
        """
        # Используем старую логику из backup файла
        backup_path = Path(__file__).parent.parent / 'serp_advanced_clusterer.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("serp_advanced_clusterer_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.AdvancedSERPClusterer()
            return temp_instance._find_similar_queries_fast(query, query_urls, url_index)
        else:
            # Fallback: простая реализация
            similar_queries = {}
            query_urls_set = set(query_urls[:top_positions])
            
            for url in query_urls_set:
                if url in url_index:
                    for other_query in url_index[url]:
                        if other_query != query:
                            if other_query not in similar_queries:
                                similar_queries[other_query] = 0
                            similar_queries[other_query] += 1
            
            return similar_queries

