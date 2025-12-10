"""Определение сообществ в графе"""

from typing import Dict
import networkx as nx
import community as community_louvain  # python-louvain


def detect_communities_louvain(
    graph: nx.Graph,
    resolution: float = 1.0
) -> Dict[int, int]:
    """
    Определяет сообщества методом Louvain
    
    Args:
        graph: Граф NetworkX
        resolution: Параметр разрешения
        
    Returns:
        Словарь {node_id: community_id}
    """
    print(f"🔄 Community Detection (Louvain, resolution={resolution})...")
    
    # Применяем алгоритм Louvain
    communities = community_louvain.best_partition(
        graph,
        weight='weight',
        resolution=resolution
    )
    
    n_communities = len(set(communities.values()))
    print(f"✓ Найдено {n_communities} сообществ")
    
    return communities


def detect_communities_label_propagation(graph: nx.Graph) -> Dict[int, int]:
    """
    Определяет сообщества методом Label Propagation
    
    Args:
        graph: Граф NetworkX
        
    Returns:
        Словарь {node_id: community_id}
    """
    print("🔄 Community Detection (Label Propagation)...")
    
    communities_generator = nx.algorithms.community.label_propagation_communities(graph)
    communities_list = list(communities_generator)
    
    # Преобразуем в словарь
    communities = {}
    for comm_id, community in enumerate(communities_list):
        for node in community:
            communities[node] = comm_id
    
    print(f"✓ Найдено {len(communities_list)} сообществ")
    
    return communities

