"""
Улучшенная SOFT-кластеризация с режимами strict/balanced/soft
Фасад для модулей кластеризации
"""

from typing import List, Dict, Set, Tuple, Optional
import pandas as pd
from collections import defaultdict

from .semantic_checker import SemanticClusterChecker
from .fast_similarity import FastSimilarityCalculator
from .serp_clustering.url_index_builder import URLIndexBuilder
from .serp_clustering.url_normalizer import URLNormalizer
from .serp_clustering.similarity_finder import SimilarityFinder
from .serp_clustering.cluster_validator import ClusterValidator
from .serp_clustering.cluster_processor import ClusterProcessor


class AdvancedSERPClusterer:
    """
    Продвинутая SERP кластеризация с контролем транзитивности
    
    Режимы:
    - STRICT: каждый запрос должен быть схож с КАЖДЫМ в кластере
    - BALANCED: запрос должен быть схож минимум с 50% запросов в кластере  
    - SOFT: достаточно схожести хотя бы с одним (транзитивное замыкание)
    """
    
    MODE_STRICT = "strict"
    MODE_BALANCED = "balanced"
    MODE_SOFT = "soft"
    
    def __init__(
        self,
        min_common_urls: int = 7,
        top_positions: int = 30,
        max_cluster_size: int = 100,
        mode: str = "balanced",
        semantic_check: bool = True,
        min_cluster_cohesion: float = 0.5,
        geo_dicts: Dict[str, Set[str]] = None
    ):
        """
        Args:
            min_common_urls: Минимум общих URL для связи (по умолчанию 7)
            top_positions: Глубина анализа SERP (по умолчанию 30)
            max_cluster_size: Максимальный размер кластера (по умолчанию 100)
            mode: Режим кластеризации (strict/balanced/soft)
            semantic_check: Проверять семантическую схожесть запросов
            min_cluster_cohesion: Мин. связность кластера (0-1) для balanced режима
            geo_dicts: Словари с географическими названиями для проверки
        """
        self.min_common_urls = min_common_urls
        self.top_positions = top_positions
        self.max_cluster_size = max_cluster_size
        self.mode = mode
        self.semantic_check = semantic_check
        self.min_cluster_cohesion = min_cluster_cohesion
        
        self.clusters = {}  # query -> cluster_id
        self.cluster_queries = defaultdict(list)  # cluster_id -> [queries]
        self.cluster_geo_cache = {}  # ОПТИМИЗАЦИЯ: cluster_id -> география (кэш)
        
        # Семантический чекер для проверки совместимости
        self.semantic_checker = SemanticClusterChecker(geo_dicts=geo_dicts) if semantic_check else None
        
        # 🚀 ОПТИМИЗАЦИЯ: Быстрый калькулятор схожести
        self.fast_similarity = FastSimilarityCalculator(
            top_positions=top_positions
        )
        
        # Кэш схожести для избежания повторных вычислений
        self.similarity_cache = {}  # (query1, query2) -> common_count
    
    def _build_url_index(self, query_urls_dict: Dict[str, List[str]]) -> Dict[str, Set[str]]:
        """Строит инвертированный индекс URL → запросы"""
        return URLIndexBuilder.build_url_index(query_urls_dict, self.top_positions)
    
    def _find_similar_queries_fast(
        self,
        query: str,
        query_urls: List[str],
        url_index: Dict[str, Set[str]]
    ) -> Dict[str, int]:
        """Быстрый поиск похожих запросов через инвертированный индекс"""
        return SimilarityFinder.find_similar_queries_fast(
            query, query_urls, url_index, self.fast_similarity, self.top_positions
        )
    
    def _are_semantically_different(self, query1: str, query2: str) -> bool:
        """Проверяет семантическую разницу между запросами"""
        if not self.semantic_checker:
            return False
        return self.semantic_checker.are_semantically_different(query1, query2)
    
    def _normalize_url(self, url: str) -> str:
        """Нормализует URL для сравнения"""
        return URLNormalizer.normalize_url(url)
    
    def extract_serp_urls(self, serp_data) -> List[str]:
        """Извлекает и нормализует URL из SERP данных"""
        return URLNormalizer.extract_serp_urls(serp_data)
    
    def _extract_domain(self, url: str) -> str:
        """Извлекает домен из URL"""
        return URLNormalizer.extract_domain(url)
    
    def calculate_similarity(self, urls1: List[str], urls2: List[str]) -> int:
        """Рассчитывает схожесть между двумя списками URL"""
        if not urls1 or not urls2:
            return 0
        return self.fast_similarity.calculate_similarity(urls1, urls2)
    
    def _can_add_to_cluster(
        self,
        query: str,
        cluster_queries: List[str],
        query_urls_dict: Dict[str, List[str]],
        query_geo_dict: Dict[str, str] = None,
        debug: bool = False,
        cluster_id: int = None
    ) -> bool:
        """Проверяет может ли запрос быть добавлен в кластер"""
        return ClusterValidator.can_add_to_cluster(
            query, cluster_queries, query_urls_dict,
            self.min_common_urls, self.mode, self.semantic_checker,
            query_geo_dict, cluster_id, self.cluster_geo_cache,
            self.similarity_cache, self.fast_similarity, debug
        )
    
    async def cluster_by_serp(
        self,
        df: pd.DataFrame,
        serp_column: str = 'serp_main_pages',
        geo_processor=None
    ) -> pd.DataFrame:
        """Выполняет продвинутую SOFT-кластеризацию запросов по SERP"""
        return await ClusterProcessor.cluster_by_serp(df, self, serp_column, geo_processor)
    
    def get_cluster_stats(self) -> Dict:
        """Получить статистику по кластерам"""
        if not self.cluster_queries:
            return {
                'total_clusters': 0,
                'total_queries': len(self.clusters),
                'avg_cluster_size': 0.0,
                'max_cluster_size': 0,
                'min_cluster_size': 0,
                'singleton_clusters': 0
            }
        
        cluster_sizes = [len(queries) for queries in self.cluster_queries.values()]
        
        return {
            'total_clusters': len(self.cluster_queries),
            'total_queries': len(self.clusters),
            'avg_cluster_size': sum(cluster_sizes) / len(cluster_sizes) if cluster_sizes else 0.0,
            'max_cluster_size': max(cluster_sizes) if cluster_sizes else 0,
            'min_cluster_size': min(cluster_sizes) if cluster_sizes else 0,
            'singleton_clusters': sum(1 for size in cluster_sizes if size == 1)
        }


__all__ = ['AdvancedSERPClusterer']
