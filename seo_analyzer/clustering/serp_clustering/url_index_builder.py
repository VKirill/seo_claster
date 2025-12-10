"""
Построение инвертированного индекса URL → запросы
"""

from typing import Dict, List, Set
from collections import defaultdict


class URLIndexBuilder:
    """Построитель инвертированного индекса URL"""
    
    @staticmethod
    def build_url_index(
        query_urls_dict: Dict[str, List[str]],
        top_positions: int = 30
    ) -> Dict[str, Set[str]]:
        """
        Строит инвертированный индекс URL → запросы.
        
        Это ускоряет поиск похожих запросов с O(n²) до O(n × k),
        где k = среднее количество URL (~30).
        
        Args:
            query_urls_dict: Словарь запрос → список URL
            top_positions: Глубина анализа SERP
            
        Returns:
            Инвертированный индекс: URL → множество запросов
        """
        url_to_queries = defaultdict(set)
        
        for query, urls in query_urls_dict.items():
            # Берем только топ-N позиций
            for url in urls[:top_positions]:
                url_to_queries[url].add(query)
        
        return url_to_queries

