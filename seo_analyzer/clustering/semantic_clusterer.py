"""Семантическая кластеризация запросов (фасад для обратной совместимости)"""

from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

from .semantic import SemanticClusterer as SemanticClustererImpl
from .semantic.analyzer import get_cluster_top_terms, get_cluster_summary, assign_cluster_names


class SemanticClusterer:
    """
    Семантическая кластеризация на основе TF-IDF
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.clustering.semantic
    """
    
    def __init__(self, config: Dict = None):
        self._clusterer = SemanticClustererImpl(config)
        # Делегируем атрибуты для обратной совместимости
        self.config = config or {}
        self.vectorizer = self._clusterer.vectorizer
        self.tfidf_matrix = self._clusterer.tfidf_matrix
        self.feature_names = self._clusterer.feature_names
        self.cluster_labels = self._clusterer.cluster_labels
        self.n_clusters = self._clusterer.n_clusters
    
    def fit_tfidf(self, texts: List[str]) -> np.ndarray:
        """Обучает TF-IDF на текстах"""
        result = self._clusterer.fit_tfidf(texts)
        self._sync_attributes()
        return result
    
    def find_optimal_clusters(self, min_clusters: int = 5, max_clusters: int = 50, method: str = 'elbow') -> int:
        """Находит оптимальное количество кластеров"""
        return self._clusterer.find_optimal_clusters(min_clusters, max_clusters, method)
    
    def cluster_kmeans(self, n_clusters: Optional[int] = None, auto_detect: bool = True) -> np.ndarray:
        """Кластеризация K-Means"""
        result = self._clusterer.cluster_kmeans(n_clusters, auto_detect)
        self._sync_attributes()
        return result
    
    def cluster_dbscan(self, eps: float = 0.3, min_samples: int = 3) -> np.ndarray:
        """Кластеризация DBSCAN"""
        result = self._clusterer.cluster_dbscan(eps, min_samples)
        self._sync_attributes()
        return result
    
    def get_cluster_top_terms(self, cluster_id: int, top_n: int = 10) -> List[Tuple[str, float]]:
        """Возвращает топ-термины для кластера"""
        return get_cluster_top_terms(
            cluster_id,
            self.cluster_labels,
            self.tfidf_matrix,
            self.feature_names,
            top_n
        )
    
    def get_cluster_summary(self) -> Dict[int, Dict]:
        """Возвращает сводку по всем кластерам"""
        return get_cluster_summary(
            self.cluster_labels,
            self.tfidf_matrix,
            self.feature_names
        )
    
    def assign_cluster_names(self, df: pd.DataFrame, use_exact_frequency: bool = True) -> pd.DataFrame:
        """Присваивает названия кластерам в DataFrame"""
        return assign_cluster_names(
            df,
            self.cluster_labels,
            self.tfidf_matrix,
            self.feature_names,
            use_exact_frequency
        )
    
    def _sync_attributes(self):
        """Синхронизирует атрибуты для обратной совместимости"""
        self.vectorizer = self._clusterer.vectorizer
        self.tfidf_matrix = self._clusterer.tfidf_matrix
        self.feature_names = self._clusterer.feature_names
        self.cluster_labels = self._clusterer.cluster_labels
        self.n_clusters = self._clusterer.n_clusters

