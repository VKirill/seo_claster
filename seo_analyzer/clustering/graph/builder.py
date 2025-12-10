"""Построение графа связей"""

from typing import Dict, List
import numpy as np
import networkx as nx
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm


def build_graph_from_similarity(
    embeddings: np.ndarray,
    queries: List[str],
    similarity_threshold: float = 0.5,
    min_edge_weight: float = 0.3
) -> nx.Graph:
    """
    Строит граф на основе матрицы схожести
    
    Args:
        embeddings: Векторные представления запросов
        queries: Список запросов
        similarity_threshold: Минимальная схожесть для ребра
        min_edge_weight: Минимальный вес ребра
        
    Returns:
        Граф NetworkX
    """
    print(f"🔄 Построение графа (порог схожести={similarity_threshold})...")
    
    # Создаем граф
    graph = nx.Graph()
    
    # Добавляем узлы
    for i, query in enumerate(queries):
        graph.add_node(i, query=query)
    
    # Вычисляем матрицу схожести
    print("  Вычисление матрицы схожести...")
    similarity_matrix = cosine_similarity(embeddings)
    
    # Добавляем ребра
    print("  Добавление ребер...")
    edges_added = 0
    
    for i in tqdm(range(len(queries)), desc="Построение ребер"):
        for j in range(i + 1, len(queries)):
            similarity = similarity_matrix[i, j]
            
            if similarity >= similarity_threshold and similarity >= min_edge_weight:
                graph.add_edge(i, j, weight=similarity)
                edges_added += 1
    
    print(f"✓ Граф построен: {len(graph.nodes)} узлов, {edges_added} ребер")
    
    return graph

