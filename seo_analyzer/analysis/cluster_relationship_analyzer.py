"""Анализ связей между кластерами для перелинковки"""

from typing import Dict, List, Tuple
import pandas as pd
from collections import defaultdict
from .cluster_data_grouper import group_clusters_data, filter_stopwords


class ClusterRelationshipAnalyzer:
    """
    Анализирует связи между кластерами для построения внутренней перелинковки.
    
    Связь между кластерами определяется через:
    1. Пересечение URL в SERP (общие страницы в выдаче)
    2. Общие ключевые слова (но разные кластеры)
    """
    
    def __init__(
        self,
        min_url_overlap: int = 3,
        min_word_overlap: int = 2,
        max_related_clusters: int = 5
    ):
        """
        Args:
            min_url_overlap: Минимум общих URL для связи между кластерами
            min_word_overlap: Минимум общих ключевых слов
            max_related_clusters: Максимум связанных кластеров для каждого
        """
        self.min_url_overlap = min_url_overlap
        self.min_word_overlap = min_word_overlap
        self.max_related_clusters = max_related_clusters
    
    def analyze_relationships(
        self,
        df: pd.DataFrame,
        cluster_column: str = 'semantic_cluster_id'
    ) -> Dict[int, List[Tuple[int, str, int]]]:
        """
        Анализирует связи между кластерами.
        
        Args:
            df: DataFrame с кластерами и SERP данными
            cluster_column: Название колонки с ID кластера
            
        Returns:
            Dict[cluster_id, List[(related_cluster_id, cluster_name, strength)]]
            где strength - сила связи (количество общих URL)
        """
        print("🔗 Анализ связей между кластерами...")
        
        if cluster_column not in df.columns:
            print("  ⚠️  Колонка кластера не найдена")
            return {}
        
        # Группируем данные по кластерам
        cluster_data = group_clusters_data(df, cluster_column)
        
        # Находим связи через SERP
        relationships = self._find_serp_relationships(cluster_data)
        
        # Дополняем связями через общие слова
        relationships = self._enhance_with_word_relationships(
            cluster_data,
            relationships
        )
        
        # Ограничиваем количество связей и сортируем по силе
        relationships = self._limit_and_sort_relationships(relationships)
        
        print(f"✓ Найдено связей: {sum(len(v) for v in relationships.values())}")
        
        return relationships
    
    def _find_serp_relationships(
        self,
        cluster_data: Dict[int, Dict]
    ) -> Dict[int, List[Tuple[int, str, int]]]:
        """Находит связи через пересечение SERP URLs"""
        relationships = defaultdict(list)
        
        cluster_ids = list(cluster_data.keys())
        
        for i, cluster_id_1 in enumerate(cluster_ids):
            for cluster_id_2 in cluster_ids[i+1:]:
                data_1 = cluster_data[cluster_id_1]
                data_2 = cluster_data[cluster_id_2]
                
                # Считаем пересечение URLs
                common_urls = data_1['urls'] & data_2['urls']
                overlap_count = len(common_urls)
                
                if overlap_count >= self.min_url_overlap:
                    # Добавляем связь в обе стороны
                    relationships[cluster_id_1].append((
                        cluster_id_2,
                        data_2['name'],
                        overlap_count
                    ))
                    relationships[cluster_id_2].append((
                        cluster_id_1,
                        data_1['name'],
                        overlap_count
                    ))
        
        return relationships
    
    def _enhance_with_word_relationships(
        self,
        cluster_data: Dict[int, Dict],
        relationships: Dict[int, List[Tuple[int, str, int]]]
    ) -> Dict[int, List[Tuple[int, str, int]]]:
        """Дополняет связи через общие ключевые слова"""
        cluster_ids = list(cluster_data.keys())
        existing_links = {
            cluster_id: {rel[0] for rel in rels}
            for cluster_id, rels in relationships.items()
        }
        
        for i, cluster_id_1 in enumerate(cluster_ids):
            for cluster_id_2 in cluster_ids[i+1:]:
                # Пропускаем если связь уже есть
                if cluster_id_2 in existing_links.get(cluster_id_1, set()):
                    continue
                
                data_1 = cluster_data[cluster_id_1]
                data_2 = cluster_data[cluster_id_2]
                
                # Считаем пересечение слов (без стоп-слов)
                common_words = filter_stopwords(
                    data_1['words'] & data_2['words']
                )
                overlap_count = len(common_words)
                
                if overlap_count >= self.min_word_overlap:
                    # Добавляем связь с меньшей силой
                    strength = overlap_count
                    
                    relationships[cluster_id_1].append((
                        cluster_id_2,
                        data_2['name'],
                        strength
                    ))
                    relationships[cluster_id_2].append((
                        cluster_id_1,
                        data_1['name'],
                        strength
                    ))
        
        return relationships
    
    def _limit_and_sort_relationships(
        self,
        relationships: Dict[int, List[Tuple[int, str, int]]]
    ) -> Dict[int, List[Tuple[int, str, int]]]:
        """Ограничивает и сортирует связи по силе"""
        limited = {}
        
        for cluster_id, relations in relationships.items():
            # Сортируем по силе связи (убывание)
            sorted_relations = sorted(
                relations,
                key=lambda x: x[2],
                reverse=True
            )
            
            # Берём топ-N
            limited[cluster_id] = sorted_relations[:self.max_related_clusters]
        
        return limited
