"""Построение графа связей запросов (фасад для обратной совместимости)"""

from typing import Dict, List, Tuple
import pandas as pd
import networkx as nx

from .graph import GraphBuilder as GraphBuilderImpl


class GraphBuilder:
    """
    Построитель графа связей запросов
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.clustering.graph
    """
    
    def __init__(self, config: Dict = None):
        self._builder = GraphBuilderImpl(config)
        # Делегируем атрибуты для обратной совместимости
        self.config = config or {}
        self.graph_config = self._builder.graph_config
        self.graph = self._builder.graph
        self.communities = self._builder.communities
        self.pagerank_scores = self._builder.pagerank_scores
    
    def build_graph_from_similarity(self, embeddings, queries: List[str], similarity_threshold: float = None, min_edge_weight: float = None) -> nx.Graph:
        """Строит граф на основе матрицы схожести"""
        result = self._builder.build_graph_from_similarity(embeddings, queries, similarity_threshold, min_edge_weight)
        self._sync_attributes()
        return result
    
    def detect_communities_louvain(self, resolution: float = None) -> Dict[int, int]:
        """Определяет сообщества методом Louvain"""
        result = self._builder.detect_communities_louvain(resolution)
        self._sync_attributes()
        return result
    
    def detect_communities_label_propagation(self) -> Dict[int, int]:
        """Определяет сообщества методом Label Propagation"""
        result = self._builder.detect_communities_label_propagation()
        self._sync_attributes()
        return result
    
    def calculate_pagerank(self, alpha: float = 0.85, max_iter: int = 100) -> Dict[int, float]:
        """Вычисляет PageRank для узлов"""
        result = self._builder.calculate_pagerank(alpha, max_iter)
        self._sync_attributes()
        return result
    
    def get_hub_nodes(self, top_n: int = 50) -> List[Tuple[int, float]]:
        """Возвращает топ хаб-узлов по PageRank"""
        return self._builder.get_hub_nodes(top_n)
    
    def get_community_info(self, queries: List[str]) -> Dict[int, Dict]:
        """Возвращает информацию о сообществах"""
        return self._builder.get_community_info(queries)
    
    def add_graph_features_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет графовые фичи в DataFrame"""
        return self._builder.add_graph_features_to_dataframe(df)
    
    def export_graph_data(self) -> Dict[str, any]:
        """Экспортирует данные графа для визуализации"""
        return self._builder.export_graph_data()
    
    def get_graph_statistics(self) -> Dict[str, any]:
        """Возвращает статистику графа"""
        return self._builder.get_graph_statistics()
    
    def _sync_attributes(self):
        """Синхронизирует атрибуты для обратной совместимости"""
        self.graph = self._builder.graph
        self.communities = self._builder.communities
        self.pagerank_scores = self._builder.pagerank_scores

