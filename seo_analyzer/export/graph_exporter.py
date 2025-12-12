"""Экспорт графов в GraphML и GEXF"""

from pathlib import Path
from typing import Optional
import networkx as nx


class GraphExporter:
    """Экспортер графов для Gephi и других инструментов"""
    
    def __init__(self):
        """Инициализация"""
        pass
    
    def export_graphml(
        self,
        graph: nx.Graph,
        output_path: Path,
        communities: Optional[dict] = None,
        pagerank: Optional[dict] = None
    ) -> bool:
        """
        Экспортирует граф в формат GraphML
        
        Args:
            graph: Граф NetworkX
            output_path: Путь для сохранения
            communities: Словарь {node_id: community_id}
            pagerank: Словарь {node_id: pagerank_score}
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт в GraphML: {output_path.name}...")
            
            # Добавляем атрибуты узлам
            if communities:
                nx.set_node_attributes(graph, communities, 'community')
            
            if pagerank:
                nx.set_node_attributes(graph, pagerank, 'pagerank')
            
            # Добавляем степень узлов
            degrees = dict(graph.degree())
            nx.set_node_attributes(graph, degrees, 'degree')
            
            # Сохраняем
            nx.write_graphml(graph, output_path)
            
            print(f"✓ GraphML экспортирован: {graph.number_of_nodes()} узлов, {graph.number_of_edges()} ребер")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта GraphML: {e}")
            return False
    
    def export_gexf(
        self,
        graph: nx.Graph,
        output_path: Path,
        communities: Optional[dict] = None,
        pagerank: Optional[dict] = None
    ) -> bool:
        """
        Экспортирует граф в формат GEXF (для Gephi)
        
        Args:
            graph: Граф NetworkX
            output_path: Путь для сохранения
            communities: Словарь {node_id: community_id}
            pagerank: Словарь {node_id: pagerank_score}
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт в GEXF: {output_path.name}...")
            
            # Добавляем атрибуты
            if communities:
                nx.set_node_attributes(graph, communities, 'community')
            
            if pagerank:
                nx.set_node_attributes(graph, pagerank, 'pagerank')
            
            # Добавляем степень
            degrees = dict(graph.degree())
            nx.set_node_attributes(graph, degrees, 'degree')
            
            # Сохраняем
            nx.write_gexf(graph, output_path)
            
            print(f"✓ GEXF экспортирован")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта GEXF: {e}")
            return False
    
    def export_edge_list(
        self,
        graph: nx.Graph,
        output_path: Path
    ) -> bool:
        """
        Экспортирует список ребер (простой формат)
        
        Args:
            graph: Граф NetworkX
            output_path: Путь для сохранения
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт списка ребер: {output_path.name}...")
            
            nx.write_edgelist(graph, output_path, data=['weight'])
            
            print(f"✓ Список ребер экспортирован")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта списка ребер: {e}")
            return False
    
    def export_adjacency_matrix(
        self,
        graph: nx.Graph,
        output_path: Path
    ) -> bool:
        """
        Экспортирует матрицу смежности
        
        Args:
            graph: Граф NetworkX
            output_path: Путь для сохранения
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт матрицы смежности: {output_path.name}...")
            
            import pandas as pd
            
            # Получаем матрицу смежности
            adj_matrix = nx.adjacency_matrix(graph)
            
            # Конвертируем в DataFrame
            node_list = list(graph.nodes())
            df = pd.DataFrame(
                adj_matrix.toarray(),
                index=node_list,
                columns=node_list
            )
            
            # Сохраняем
            df.to_csv(output_path)
            
            print(f"✓ Матрица смежности экспортирована")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта матрицы: {e}")
            return False






