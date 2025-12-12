"""Дедупликация запросов"""

from typing import Dict, List, Tuple
import pandas as pd
from collections import defaultdict


class QueryDeduplicator:
    """Дедупликатор запросов на основе нормализованных форм"""
    
    def __init__(self):
        """Инициализация дедупликатора"""
        self.duplicates_count = 0
        self.unique_count = 0
    
    def deduplicate(
        self,
        df: pd.DataFrame,
        normalized_column: str = 'normalized',
        original_column: str = 'keyword',
        freq_column: str = 'frequency_exact'
    ) -> pd.DataFrame:
        """
        Дедуплицирует запросы, оставляя с максимальной частотностью
        
        Args:
            df: DataFrame с запросами
            normalized_column: Колонка с нормализованными запросами
            original_column: Колонка с оригинальными запросами
            freq_column: Колонка с частотностью
            
        Returns:
            Дедуплицированный DataFrame
        """
        # Группируем по нормализованной форме
        grouped = df.groupby(normalized_column)
        
        # Для каждой группы выбираем запись с максимальной частотностью
        def select_best(group):
            if freq_column in group.columns:
                # Выбираем с максимальной частотностью
                return group.loc[group[freq_column].idxmax()]
            else:
                # Если нет частотности, берем первый
                return group.iloc[0]
        
        result = grouped.apply(select_best).reset_index(drop=True)
        
        self.duplicates_count = len(df) - len(result)
        self.unique_count = len(result)
        
        return result
    
    def find_duplicates(
        self,
        df: pd.DataFrame,
        normalized_column: str = 'normalized'
    ) -> Dict[str, List[str]]:
        """
        Находит все дубликаты
        
        Args:
            df: DataFrame с запросами
            normalized_column: Колонка с нормализованными запросами
            
        Returns:
            Словарь {normalized: [original_queries]}
        """
        duplicates = defaultdict(list)
        
        for _, row in df.iterrows():
            normalized = row[normalized_column]
            original = row.get('keyword', row.get('original', ''))
            duplicates[normalized].append(original)
        
        # Фильтруем только те, где больше 1 варианта
        duplicates = {k: v for k, v in duplicates.items() if len(v) > 1}
        
        return dict(duplicates)
    
    def get_deduplication_stats(self) -> Dict[str, int]:
        """Возвращает статистику дедупликации"""
        return {
            'total_duplicates_removed': self.duplicates_count,
            'unique_queries': self.unique_count,
        }
    
    def merge_similar_queries(
        self,
        df: pd.DataFrame,
        similarity_threshold: float = 0.9
    ) -> pd.DataFrame:
        """
        Объединяет очень похожие запросы (для продвинутой дедупликации)
        
        Args:
            df: DataFrame
            similarity_threshold: Порог схожести
            
        Returns:
            DataFrame с объединенными запросами
        """
        # Базовая реализация - по длине Левенштейна
        # Для более точной нужны embeddings
        from difflib import SequenceMatcher
        
        def similar(a: str, b: str) -> float:
            return SequenceMatcher(None, a, b).ratio()
        
        # Сортируем для стабильности
        df = df.sort_values('keyword').reset_index(drop=True)
        
        merged_indices = set()
        groups = []
        
        for i in range(len(df)):
            if i in merged_indices:
                continue
            
            group = [i]
            query_i = df.loc[i, 'keyword'].lower()
            
            for j in range(i + 1, len(df)):
                if j in merged_indices:
                    continue
                
                query_j = df.loc[j, 'keyword'].lower()
                
                if similar(query_i, query_j) >= similarity_threshold:
                    group.append(j)
                    merged_indices.add(j)
            
            groups.append(group)
        
        # Для каждой группы выбираем лучший запрос
        result_rows = []
        
        for group in groups:
            group_df = df.loc[group]
            
            # Выбираем с максимальной частотностью
            if 'frequency_exact' in group_df.columns:
                best = group_df.loc[group_df['frequency_exact'].idxmax()]
            else:
                best = group_df.iloc[0]
            
            result_rows.append(best)
        
        return pd.DataFrame(result_rows).reset_index(drop=True)






