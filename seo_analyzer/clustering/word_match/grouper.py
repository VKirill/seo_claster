"""Группировка результатов кластеризации"""

from typing import Dict, List, Optional
import pandas as pd

from .cluster_builder import build_initial_clusters, filter_and_number_clusters
from .cluster_processor import strengthen_cluster_links, redistribute_orphans
from .cluster_exporter import (
    get_cluster_stats,
    add_to_dataframe,
    get_cluster_details,
    export_clusters
)


class WordMatchClusterer:
    """
    Кластеризатор по совпадениям слов
    
    Алгоритм аналогичен KeyCollector:
    - Подсчитывает количество совпадающих слов между фразами
    - Группирует фразы с минимальным порогом совпадений
    - Перераспределяет фразы между группами при усилении связей
    """
    
    def __init__(
        self,
        min_match_strength: int = 2,
        min_group_size: int = 2,
        strengthen_links: bool = True,
        exclude_stopwords: bool = True,
        use_lemmatization: bool = True
    ):
        """
        Инициализация кластеризатора
        
        Args:
            min_match_strength: Сила группировки (минимум совпадений слов)
            min_group_size: Минимальный размер группы
            strengthen_links: Усиливать связи в группах (перераспределение)
            exclude_stopwords: Исключать стоп-слова из подсчета совпадений
            use_lemmatization: Использовать леммы для сравнения
        """
        self.min_match_strength = min_match_strength
        self.min_group_size = min_group_size
        self.strengthen_links = strengthen_links
        self.exclude_stopwords = exclude_stopwords
        self.use_lemmatization = use_lemmatization
        
        self.clusters = []
        self.query_to_cluster = {}
    
    def cluster_queries(
        self,
        queries: List[str],
        frequencies: Optional[Dict[str, int]] = None
    ) -> List[Dict]:
        """
        Кластеризует запросы по совпадениям слов
        
        Args:
            queries: Список запросов
            frequencies: Словарь {query: frequency} для сортировки
            
        Returns:
            Список кластеров с метаданными
        """
        print(f"🔄 Кластеризация по совпадениям слов...")
        print(f"   Параметры: сила={self.min_match_strength}, мин.размер={self.min_group_size}")
        
        # Этап 1: Первичная группировка
        clusters = build_initial_clusters(
            queries,
            frequencies,
            self.min_match_strength,
            self.exclude_stopwords
        )
        print(f"   Этап 1: создано {len(clusters)} первичных групп")
        
        # Этап 2: Усиление связей (если включено)
        if self.strengthen_links:
            print(f"   Этап 2: усиление связей...")
            clusters = strengthen_cluster_links(clusters, self.exclude_stopwords)
        
        # Этап 3: Фильтрация по минимальному размеру
        valid_clusters, orphan_queries = filter_and_number_clusters(
            clusters,
            self.min_group_size
        )
        print(f"   Этап 3: осталось {len(valid_clusters)} групп >= {self.min_group_size} фраз")
        
        # Этап 4: Перераспределение одиночных фраз
        if orphan_queries:
            print(f"   Этап 4: перераспределение {len(orphan_queries)} одиночных фраз...")
            redistributed = redistribute_orphans(
                orphan_queries,
                valid_clusters,
                self.min_match_strength,
                self.exclude_stopwords
            )
            print(f"   Перераспределено: {redistributed}/{len(orphan_queries)}")
        
        self.clusters = valid_clusters
        
        # Создаем маппинг query -> cluster_id
        self.query_to_cluster = {}
        for cluster in self.clusters:
            for query in cluster['queries']:
                self.query_to_cluster[query] = cluster['cluster_id']
        
        print(f"✓ Кластеризация завершена: {len(self.clusters)} групп")
        return self.clusters
    
    def get_cluster_stats(self) -> Dict:
        """Возвращает статистику по кластерам"""
        return get_cluster_stats(self.clusters, self.query_to_cluster)
    
    def add_to_dataframe(
        self,
        df: pd.DataFrame,
        query_column: str = 'keyword',
        cluster_column: str = 'word_match_cluster_id',
        cluster_name_column: str = 'word_match_cluster_name'
    ) -> pd.DataFrame:
        """Добавляет результаты кластеризации в DataFrame"""
        return add_to_dataframe(
            df,
            self.clusters,
            self.query_to_cluster,
            query_column,
            cluster_column,
            cluster_name_column
        )
    
    def get_cluster_details(self, cluster_id: int) -> Optional[Dict]:
        """Возвращает детали конкретного кластера"""
        return get_cluster_details(self.clusters, cluster_id)
    
    def export_clusters(self) -> pd.DataFrame:
        """Экспортирует кластеры в DataFrame"""
        return export_clusters(self.clusters)

