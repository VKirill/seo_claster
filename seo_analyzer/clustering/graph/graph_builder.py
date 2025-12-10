"""Основной класс построителя графа"""

from typing import Dict, List
import pandas as pd
import networkx as nx

from .builder import build_graph_from_similarity
from .communities import detect_communities_louvain, detect_communities_label_propagation
from .analyzer import (
    calculate_pagerank,
    get_hub_nodes,
    get_community_info,
    add_graph_features_to_dataframe,
    export_graph_data,
    get_graph_statistics
)


class GraphBuilder:
    """Построитель графа связей запросов"""
    
    def __init__(self, config: Dict = None):
        """
        Инициализация
        
        Args:
            config: Конфигурация графа
        """
        self.config = config or {}
        self.graph_config = self.config.get('graph', {})
        
        self.graph = None
        self.communities = None
        self.pagerank_scores = None
    
    def build_graph_from_similarity(
        self,
        embeddings,
        queries: List[str],
        similarity_threshold: float = None,
        min_edge_weight: float = None
    ) -> nx.Graph:
        """Строит граф на основе матрицы схожести"""
        if similarity_threshold is None:
            similarity_threshold = self.graph_config.get('similarity_threshold', 0.5)
        
        if min_edge_weight is None:
            min_edge_weight = self.graph_config.get('min_edge_weight', 0.3)
        
        self.graph = build_graph_from_similarity(
            embeddings,
            queries,
            similarity_threshold,
            min_edge_weight
        )
        
        return self.graph
    
    def detect_communities_louvain(self, resolution: float = None) -> Dict[int, int]:
        """Определяет сообщества методом Louvain"""
        if self.graph is None:
            raise ValueError("Сначала нужно построить граф")
        
        if resolution is None:
            resolution = self.graph_config.get('community_resolution', 1.0)
        
        self.communities = detect_communities_louvain(self.graph, resolution)
        return self.communities
    
    def detect_communities_label_propagation(self) -> Dict[int, int]:
        """Определяет сообщества методом Label Propagation"""
        if self.graph is None:
            raise ValueError("Сначала нужно построить граф")
        
        self.communities = detect_communities_label_propagation(self.graph)
        return self.communities
    
    def calculate_pagerank(self, alpha: float = 0.85, max_iter: int = 100) -> Dict[int, float]:
        """Вычисляет PageRank для узлов"""
        if self.graph is None:
            raise ValueError("Сначала нужно построить граф")
        
        self.pagerank_scores = calculate_pagerank(self.graph, alpha, max_iter)
        return self.pagerank_scores
    
    def get_hub_nodes(self, top_n: int = 50) -> List[tuple]:
        """Возвращает топ хаб-узлов по PageRank"""
        if self.pagerank_scores is None:
            self.calculate_pagerank()
        
        return get_hub_nodes(self.pagerank_scores, top_n)
    
    def get_community_info(self, queries: List[str]) -> Dict[int, Dict]:
        """Возвращает информацию о сообществах"""
        if self.communities is None:
            raise ValueError("Сначала нужно найти сообщества")
        
        return get_community_info(self.communities, queries, self.pagerank_scores)
    
    def add_graph_features_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет графовые фичи в DataFrame"""
        if self.communities is None:
            print("⚠️ Сообщества не найдены, пропускаем")
            return df
        
        return add_graph_features_to_dataframe(
            df,
            self.communities,
            self.pagerank_scores,
            self.graph
        )
    
    def export_graph_data(self) -> Dict[str, any]:
        """Экспортирует данные графа для визуализации"""
        if self.graph is None:
            return {}
        
        return export_graph_data(self.graph, self.communities, self.pagerank_scores)
    
    def get_graph_statistics(self) -> Dict[str, any]:
        """Возвращает статистику графа"""
        if self.graph is None:
            return {}
        
        return get_graph_statistics(self.graph, self.communities)

