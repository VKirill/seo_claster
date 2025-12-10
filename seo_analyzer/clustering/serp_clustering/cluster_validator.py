"""
Валидация добавления запроса в кластер
"""

import sys
import importlib.util
from pathlib import Path
from typing import Dict, List


class ClusterValidator:
    """Валидатор кластеров"""
    
    @staticmethod
    def can_add_to_cluster(
        query: str,
        cluster_queries: List[str],
        query_urls_dict: Dict[str, List[str]],
        min_common_urls: int,
        mode: str,
        semantic_checker=None,
        query_geo_dict: Dict[str, str] = None,
        cluster_id: int = None,
        cluster_geo_cache: Dict = None,
        similarity_cache: Dict = None,
        fast_similarity_calculator=None,
        debug: bool = False
    ) -> bool:
        """
        Проверяет может ли запрос быть добавлен в кластер
        
        Args:
            query: Запрос для проверки
            cluster_queries: Запросы в кластере
            query_urls_dict: Словарь запрос → URL
            min_common_urls: Минимум общих URL
            mode: Режим кластеризации (strict/balanced/soft)
            semantic_checker: Семантический чекер
            query_geo_dict: Словарь запрос → география
            cluster_id: ID кластера
            cluster_geo_cache: Кэш географии кластеров
            similarity_cache: Кэш схожести
            fast_similarity_calculator: Калькулятор схожести
            debug: Режим отладки
            
        Returns:
            True если запрос может быть добавлен
        """
        # Используем старую логику из backup файла
        backup_path = Path(__file__).parent.parent / 'serp_advanced_clusterer.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("serp_advanced_clusterer_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.AdvancedSERPClusterer(
                min_common_urls=min_common_urls,
                mode=mode,
                semantic_check=semantic_checker is not None
            )
            temp_instance.semantic_checker = semantic_checker
            temp_instance.cluster_geo_cache = cluster_geo_cache or {}
            temp_instance.similarity_cache = similarity_cache or {}
            temp_instance.fast_similarity = fast_similarity_calculator
            return temp_instance._can_add_to_cluster(
                query, cluster_queries, query_urls_dict, query_geo_dict, debug, cluster_id
            )
        else:
            # Fallback: простая проверка
            return len(cluster_queries) == 0

