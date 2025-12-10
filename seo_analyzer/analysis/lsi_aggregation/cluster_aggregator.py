"""
Агрегация LSI фраз на уровне кластеров
"""

import pandas as pd
from typing import Dict, List, Any

from .phrase_extractor import PhraseExtractor
from .phrase_processor import PhraseProcessor


class ClusterAggregator:
    """Агрегатор LSI фраз для кластеров"""
    
    @staticmethod
    def aggregate_cluster_lsi(
        df: pd.DataFrame,
        cluster_column: str = 'semantic_cluster_id',
        top_n_per_cluster: int = 10
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Агрегировать LSI фразы для каждого кластера
        
        Args:
            df: DataFrame с колонками [cluster_column, 'lsi_phrases']
            cluster_column: Название колонки с ID кластера
            top_n_per_cluster: Количество топ LSI фраз на кластер
            
        Returns:
            Dict {cluster_id: [top_lsi_phrases]}
        """
        if cluster_column not in df.columns:
            print(f"⚠️  Колонка '{cluster_column}' не найдена")
            return {}
        
        if 'lsi_phrases' not in df.columns:
            print("⚠️  Колонка 'lsi_phrases' не найдена")
            return {}
        
        cluster_lsi = {}
        clusters_without_lsi = []
        clusters_with_empty_lsi = []
        
        # Группируем по кластерам
        for cluster_id, group in df.groupby(cluster_column):
            # Собираем все LSI фразы из всех запросов кластера
            all_lsi = []
            queries_with_lsi = 0
            queries_without_lsi = 0
            
            for idx, row in group.iterrows():
                lsi_list = row['lsi_phrases']
                
                # Извлекаем фразы используя PhraseExtractor
                phrases = PhraseExtractor.extract_phrases(lsi_list)
                
                if phrases:
                    all_lsi.extend(phrases)
                    queries_with_lsi += 1
                else:
                    queries_without_lsi += 1
            
            # Агрегируем фразы
            if all_lsi:
                aggregated = PhraseProcessor.aggregate_phrases(
                    all_lsi, top_n_per_cluster
                )
                cluster_lsi[cluster_id] = aggregated
            else:
                cluster_lsi[cluster_id] = []
                if queries_without_lsi > 0:
                    clusters_without_lsi.append((cluster_id, len(group), queries_without_lsi))
                if queries_with_lsi == 0:
                    clusters_with_empty_lsi.append((cluster_id, len(group)))
        
        # Диагностика
        if clusters_without_lsi:
            print(f"⚠️  Найдено {len(clusters_without_lsi)} кластеров, где не у всех запросов есть LSI фразы")
            if len(clusters_without_lsi) <= 10:
                for cid, total, without in clusters_without_lsi[:10]:
                    print(f"   Кластер {cid}: {without}/{total} запросов без LSI")
        
        if clusters_with_empty_lsi:
            print(f"⚠️  Найдено {len(clusters_with_empty_lsi)} кластеров без LSI фраз (у всех запросов пустые LSI)")
            if len(clusters_with_empty_lsi) <= 10:
                for cid, total in clusters_with_empty_lsi[:10]:
                    print(f"   Кластер {cid}: {total} запросов, все без LSI")
        
        return cluster_lsi

