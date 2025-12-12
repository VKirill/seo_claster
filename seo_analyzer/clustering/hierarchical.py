"""Иерархическая кластеризация запросов"""

from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
from scipy.cluster.hierarchy import dendrogram, linkage
import matplotlib.pyplot as plt


class HierarchicalClusterer:
    """Иерархическая кластеризация для многоуровневой структуры"""
    
    def __init__(self, config: Dict = None):
        """
        Инициализация
        
        Args:
            config: Конфигурация кластеризации
        """
        self.config = config or {}
        self.hierarchical_config = self.config.get('hierarchical', {})
        
        self.labels_level1 = None
        self.labels_level2 = None
        self.labels_level3 = None
        self.linkage_matrix = None
    
    def fit_hierarchical(
        self,
        distance_matrix: np.ndarray,
        n_clusters: Optional[int] = None,
        linkage_method: str = 'ward',
        distance_threshold: Optional[float] = None
    ) -> np.ndarray:
        """
        Обучает иерархическую кластеризацию
        
        Args:
            distance_matrix: Матрица расстояний или признаков
            n_clusters: Количество кластеров (если None, используется distance_threshold)
            linkage_method: Метод связывания ('ward', 'complete', 'average')
            distance_threshold: Порог расстояния для автоопределения кластеров
            
        Returns:
            Массив меток кластеров
        """
        print(f"🔄 Иерархическая кластеризация (linkage={linkage_method})...")
        
        if n_clusters is None and distance_threshold is None:
            distance_threshold = self.hierarchical_config.get('distance_threshold', 1.5)
        
        clustering = AgglomerativeClustering(
            n_clusters=n_clusters,
            distance_threshold=distance_threshold,
            linkage=linkage_method
        )
        
        labels = clustering.fit_predict(distance_matrix)
        
        n_clusters_found = len(set(labels))
        print(f"✓ Создано {n_clusters_found} кластеров")
        
        return labels
    
    def build_multilevel_hierarchy(
        self,
        distance_matrix: np.ndarray,
        levels: List[int] = None
    ) -> Dict[str, np.ndarray]:
        """
        Строит многоуровневую иерархию
        
        Args:
            distance_matrix: Матрица расстояний
            levels: Список количества кластеров для каждого уровня [level1, level2, level3]
            
        Returns:
            Словарь с метками для каждого уровня
        """
        if levels is None:
            levels = [5, 15, 30]  # По умолчанию 3 уровня
        
        print(f"🔄 Построение {len(levels)}-уровневой иерархии...")
        
        result = {}
        
        for i, n_clusters in enumerate(levels, 1):
            print(f"  Уровень {i}: {n_clusters} кластеров")
            
            clustering = AgglomerativeClustering(
                n_clusters=n_clusters,
                linkage='ward'
            )
            
            labels = clustering.fit_predict(distance_matrix)
            result[f'level{i}'] = labels
            
            if i == 1:
                self.labels_level1 = labels
            elif i == 2:
                self.labels_level2 = labels
            elif i == 3:
                self.labels_level3 = labels
        
        print("✓ Иерархия построена")
        return result
    
    def compute_linkage_matrix(
        self,
        distance_matrix: np.ndarray,
        method: str = 'ward'
    ) -> np.ndarray:
        """
        Вычисляет матрицу связей для дендрограммы
        
        Args:
            distance_matrix: Матрица признаков
            method: Метод связывания
            
        Returns:
            Матрица связей
        """
        print("🔄 Вычисление матрицы связей...")
        self.linkage_matrix = linkage(distance_matrix, method=method)
        print("✓ Матрица связей вычислена")
        return self.linkage_matrix
    
    def plot_dendrogram(
        self,
        save_path: Optional[str] = None,
        max_d: Optional[float] = None,
        truncate_mode: Optional[str] = 'lastp',
        p: int = 30
    ):
        """
        Строит дендрограмму
        
        Args:
            save_path: Путь для сохранения изображения
            max_d: Максимальное расстояние для отрисовки линии порога
            truncate_mode: Режим обрезки ('lastp', 'level', None)
            p: Параметр обрезки
        """
        if self.linkage_matrix is None:
            raise ValueError("Сначала нужно вычислить матрицу связей")
        
        plt.figure(figsize=(15, 8))
        
        dendrogram(
            self.linkage_matrix,
            truncate_mode=truncate_mode,
            p=p,
            show_leaf_counts=True,
            leaf_font_size=10
        )
        
        if max_d:
            plt.axhline(y=max_d, c='red', linestyle='--', label=f'Threshold: {max_d}')
            plt.legend()
        
        plt.title('Hierarchical Clustering Dendrogram')
        plt.xlabel('Query Index or Cluster Size')
        plt.ylabel('Distance')
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"✓ Дендрограмма сохранена: {save_path}")
        else:
            plt.show()
        
        plt.close()
    
    def add_hierarchy_to_dataframe(
        self,
        df: pd.DataFrame,
        hierarchy: Dict[str, np.ndarray]
    ) -> pd.DataFrame:
        """
        Добавляет иерархические метки в DataFrame
        
        Args:
            df: DataFrame
            hierarchy: Словарь с метками уровней
            
        Returns:
            DataFrame с добавленными колонками
        """
        for level_name, labels in hierarchy.items():
            column_name = f'hierarchical_{level_name}'
            df[column_name] = labels
        
        return df
    
    def get_cluster_hierarchy_summary(
        self,
        df: pd.DataFrame,
        level_column: str = 'hierarchical_level1'
    ) -> Dict[int, Dict]:
        """
        Возвращает сводку по кластерам уровня
        
        Args:
            df: DataFrame с данными
            level_column: Колонка с метками уровня
            
        Returns:
            Словарь с информацией о кластерах
        """
        summary = {}
        
        unique_clusters = df[level_column].unique()
        
        for cluster_id in unique_clusters:
            cluster_df = df[df[level_column] == cluster_id]
            
            summary[int(cluster_id)] = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_df),
                'avg_frequency': cluster_df.get('frequency_world', pd.Series([0])).mean(),
                'example_queries': cluster_df['keyword'].head(5).tolist(),
            }
        
        return summary
    
    def find_optimal_n_clusters(
        self,
        distance_matrix: np.ndarray,
        min_clusters: int = 2,
        max_clusters: int = 20
    ) -> int:
        """
        Находит оптимальное количество кластеров
        
        Args:
            distance_matrix: Матрица признаков
            min_clusters: Минимум кластеров
            max_clusters: Максимум кластеров
            
        Returns:
            Оптимальное количество
        """
        print("🔍 Поиск оптимального числа кластеров...")
        
        silhouette_scores = []
        
        for n in range(min_clusters, min(max_clusters + 1, len(distance_matrix))):
            clustering = AgglomerativeClustering(n_clusters=n, linkage='ward')
            labels = clustering.fit_predict(distance_matrix)
            
            if len(set(labels)) > 1:
                score = silhouette_score(distance_matrix, labels, sample_size=1000)
                silhouette_scores.append((n, score))
        
        if silhouette_scores:
            optimal_n = max(silhouette_scores, key=lambda x: x[1])[0]
            print(f"✓ Оптимальное количество: {optimal_n}")
            return optimal_n
        
        return (min_clusters + max_clusters) // 2






