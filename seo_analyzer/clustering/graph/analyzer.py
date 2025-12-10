"""Анализ графа и вычисление метрик"""

from typing import Dict, List, Tuple
from collections import defaultdict
import numpy as np
import pandas as pd
import networkx as nx


def calculate_pagerank(
    graph: nx.Graph,
    alpha: float = 0.85,
    max_iter: int = 100
) -> Dict[int, float]:
    """
    Вычисляет PageRank для узлов
    
    Args:
        graph: Граф NetworkX
        alpha: Damping factor
        max_iter: Максимум итераций
        
    Returns:
        Словарь {node_id: pagerank_score}
    """
    print("🔄 Вычисление PageRank...")
    
    pagerank_scores = nx.pagerank(
        graph,
        alpha=alpha,
        max_iter=max_iter,
        weight='weight'
    )
    
    print("✓ PageRank вычислен")
    
    return pagerank_scores


def get_hub_nodes(
    pagerank_scores: Dict[int, float],
    top_n: int = 50
) -> List[Tuple[int, float]]:
    """
    Возвращает топ хаб-узлов по PageRank
    
    Args:
        pagerank_scores: Словарь PageRank скоров
        top_n: Количество узлов
        
    Returns:
        Список (node_id, pagerank_score)
    """
    sorted_nodes = sorted(
        pagerank_scores.items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    return sorted_nodes[:top_n]


def get_community_info(
    communities: Dict[int, int],
    queries: List[str],
    pagerank_scores: Dict[int, float] = None
) -> Dict[int, Dict]:
    """
    Возвращает информацию о сообществах
    
    Args:
        communities: Словарь {node_id: community_id}
        queries: Список запросов
        pagerank_scores: Словарь PageRank скоров (опционально)
        
    Returns:
        Словарь с информацией о сообществах
    """
    community_info = {}
    
    # Группируем узлы по сообществам
    comm_nodes = defaultdict(list)
    
    for node_id, comm_id in communities.items():
        comm_nodes[comm_id].append(node_id)
    
    # Собираем информацию о каждом сообществе
    for comm_id, nodes in comm_nodes.items():
        # Запросы в сообществе
        community_queries = [queries[node_id] for node_id in nodes]
        
        # Средний PageRank
        if pagerank_scores:
            avg_pagerank = np.mean([pagerank_scores.get(node_id, 0) for node_id in nodes])
        else:
            avg_pagerank = 0
        
        community_info[comm_id] = {
            'community_id': comm_id,
            'size': len(nodes),
            'queries': community_queries[:10],  # Первые 10
            'avg_pagerank': avg_pagerank,
            'node_ids': nodes,
        }
    
    return community_info


def add_graph_features_to_dataframe(
    df: pd.DataFrame,
    communities: Dict[int, int],
    pagerank_scores: Dict[int, float] = None,
    graph: nx.Graph = None
) -> pd.DataFrame:
    """
    Добавляет графовые фичи в DataFrame
    
    Args:
        df: DataFrame с запросами
        communities: Словарь сообществ
        pagerank_scores: Словарь PageRank скоров (опционально)
        graph: Граф NetworkX (опционально)
        
    Returns:
        DataFrame с добавленными колонками
    """
    print("🔄 Добавление графовых фичей...")
    
    # Добавляем ID сообщества
    df['graph_community_id'] = df.index.map(lambda x: communities.get(x, -1))
    
    # Добавляем PageRank
    if pagerank_scores:
        df['pagerank_score'] = df.index.map(lambda x: pagerank_scores.get(x, 0))
    
    # Добавляем степень узла (количество связей)
    if graph:
        degrees = dict(graph.degree())
        df['node_degree'] = df.index.map(lambda x: degrees.get(x, 0))
    
    print("✓ Графовые фичи добавлены")
    
    return df


def export_graph_data(
    graph: nx.Graph,
    communities: Dict[int, int] = None,
    pagerank_scores: Dict[int, float] = None
) -> Dict[str, any]:
    """
    Экспортирует данные графа для визуализации
    
    Args:
        graph: Граф NetworkX
        communities: Словарь сообществ (опционально)
        pagerank_scores: Словарь PageRank скоров (опционально)
        
    Returns:
        Словарь с данными графа
    """
    nodes_data = []
    for node_id in graph.nodes():
        node_data = {
            'id': int(node_id),
            'query': graph.nodes[node_id].get('query', ''),
            'community': communities.get(node_id, -1) if communities else -1,
            'pagerank': pagerank_scores.get(node_id, 0) if pagerank_scores else 0,
            'degree': graph.degree(node_id),
        }
        nodes_data.append(node_data)
    
    edges_data = []
    for source, target, data in graph.edges(data=True):
        edge_data = {
            'source': int(source),
            'target': int(target),
            'weight': data.get('weight', 1.0),
        }
        edges_data.append(edge_data)
    
    return {
        'nodes': nodes_data,
        'edges': edges_data,
        'n_nodes': len(nodes_data),
        'n_edges': len(edges_data),
        'n_communities': len(set(communities.values())) if communities else 0,
    }


def get_graph_statistics(graph: nx.Graph, communities: Dict[int, int] = None) -> Dict[str, any]:
    """
    Возвращает статистику графа
    
    Args:
        graph: Граф NetworkX
        communities: Словарь сообществ (опционально)
        
    Returns:
        Словарь со статистикой
    """
    stats = {
        'n_nodes': graph.number_of_nodes(),
        'n_edges': graph.number_of_edges(),
        'density': nx.density(graph),
        'avg_degree': sum(dict(graph.degree()).values()) / graph.number_of_nodes(),
    }
    
    # Компоненты связности
    if not nx.is_connected(graph):
        stats['n_connected_components'] = nx.number_connected_components(graph)
        stats['largest_component_size'] = len(max(nx.connected_components(graph), key=len))
    else:
        stats['n_connected_components'] = 1
        stats['largest_component_size'] = stats['n_nodes']
    
    # Сообщества
    if communities:
        stats['n_communities'] = len(set(communities.values()))
    
    return stats

