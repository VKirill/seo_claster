"""Кластеризация по совпадениям слов (фасад для обратной совместимости)"""

from typing import Dict, List, Optional
import pandas as pd

from .word_match import WordMatchClusterer as WordMatchClustererImpl


class WordMatchClusterer:
    """
    Кластеризатор по совпадениям слов
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.clustering.word_match
    """
    
    def __init__(
        self,
        min_match_strength: int = 2,
        min_group_size: int = 2,
        strengthen_links: bool = True,
        exclude_stopwords: bool = True,
        use_lemmatization: bool = True
    ):
        self._clusterer = WordMatchClustererImpl(
            min_match_strength,
            min_group_size,
            strengthen_links,
            exclude_stopwords,
            use_lemmatization
        )
        # Делегируем атрибуты для обратной совместимости
        self.min_match_strength = min_match_strength
        self.min_group_size = min_group_size
        self.strengthen_links = strengthen_links
        self.exclude_stopwords = exclude_stopwords
        self.use_lemmatization = use_lemmatization
        self.clusters = self._clusterer.clusters
        self.query_to_cluster = self._clusterer.query_to_cluster
    
    def cluster_queries(self, queries: List[str], frequencies: Optional[Dict[str, int]] = None) -> List[Dict]:
        """Кластеризует запросы по совпадениям слов"""
        result = self._clusterer.cluster_queries(queries, frequencies)
        self.clusters = self._clusterer.clusters
        self.query_to_cluster = self._clusterer.query_to_cluster
        return result
    
    def get_cluster_stats(self) -> Dict:
        """Возвращает статистику по кластерам"""
        return self._clusterer.get_cluster_stats()
    
    def add_to_dataframe(self, df: pd.DataFrame, query_column: str = 'keyword', cluster_column: str = 'word_match_cluster_id', cluster_name_column: str = 'word_match_cluster_name') -> pd.DataFrame:
        """Добавляет результаты кластеризации в DataFrame"""
        return self._clusterer.add_to_dataframe(df, query_column, cluster_column, cluster_name_column)
    
    def get_cluster_details(self, cluster_id: int) -> Optional[Dict]:
        """Возвращает детали конкретного кластера"""
        return self._clusterer.get_cluster_details(cluster_id)
    
    def export_clusters(self) -> pd.DataFrame:
        """Экспортирует кластеры в DataFrame"""
        return self._clusterer.export_clusters()
