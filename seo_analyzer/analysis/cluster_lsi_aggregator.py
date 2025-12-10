"""
Cluster LSI Aggregator Module
Агрегация LSI фраз на уровне кластеров
Фасад для модулей агрегации
"""

import pandas as pd
from typing import Dict, List, Any

from .lsi_aggregation.cluster_aggregator import ClusterAggregator
from .lsi_aggregation.phrase_extractor import PhraseExtractor
from .lsi_aggregation.frequency_calculator import FrequencyCalculator


class ClusterLSIAggregator:
    """Агрегатор LSI фраз для кластеров"""
    
    def __init__(self, top_n_per_cluster: int = 10):
        """
        Args:
            top_n_per_cluster: Количество топ LSI фраз на кластер
        """
        self.top_n_per_cluster = top_n_per_cluster
    
    def aggregate_cluster_lsi(
        self,
        df: pd.DataFrame,
        cluster_column: str = 'semantic_cluster_id'
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Агрегировать LSI фразы для каждого кластера
        
        Args:
            df: DataFrame с колонками [cluster_column, 'lsi_phrases']
            cluster_column: Название колонки с ID кластера
            
        Returns:
            Dict {cluster_id: [top_lsi_phrases]}
        """
        return ClusterAggregator.aggregate_cluster_lsi(
            df, cluster_column, self.top_n_per_cluster
        )
    
    def add_cluster_lsi_to_dataframe(
        self,
        df: pd.DataFrame,
        cluster_lsi: Dict[int, List[Dict[str, Any]]],
        cluster_column: str = 'semantic_cluster_id'
    ) -> pd.DataFrame:
        """
        Добавить агрегированные LSI фразы в DataFrame
        
        Args:
            df: DataFrame
            cluster_lsi: Агрегированные LSI по кластерам
            cluster_column: Колонка с ID кластера
            
        Returns:
            DataFrame с новой колонкой 'cluster_lsi_phrases'
        """
        # Создаем маппинг cluster_id -> LSI phrases
        cluster_to_lsi = {}
        cluster_to_lsi_str = {}
        
        for cluster_id, lsi_list in cluster_lsi.items():
            # Сохраняем топ-30 как список словарей (для JSON/Excel)
            cluster_to_lsi[cluster_id] = lsi_list[:30] if lsi_list else []
            
            # Также создаем строковое представление (для CSV)
            top_phrases = []
            for item in lsi_list[:30]:
                if isinstance(item, dict):
                    phrase = item.get('phrase', '')
                    if phrase:
                        top_phrases.append(phrase)
                elif isinstance(item, str):
                    if item:
                        top_phrases.append(item)
            cluster_to_lsi_str[cluster_id] = ', '.join(top_phrases) if top_phrases else ''
        
        # Добавляем колонку с полными данными (список словарей)
        df['cluster_lsi_phrases'] = df[cluster_column].map(cluster_to_lsi)
        
        # Диагностика: проверяем сколько записей получили LSI фразы
        mapped_count = df['cluster_lsi_phrases'].notna().sum()
        empty_count = (df['cluster_lsi_phrases'].isna() | (df['cluster_lsi_phrases'] == '')).sum()
        
        # Заполняем пустые значения пустыми списками
        df['cluster_lsi_phrases'] = df['cluster_lsi_phrases'].fillna('').apply(
            lambda x: x if isinstance(x, list) else []
        )
        
        # Проверяем сколько записей имеют непустые LSI фразы после заполнения
        non_empty_count = df['cluster_lsi_phrases'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum()
        
        # Диагностика для кластеров без LSI
        clusters_without_lsi = []
        queries_without_cluster = 0
        queries_with_cluster_but_no_lsi = 0
        
        for cluster_id in df[cluster_column].unique():
            cluster_df = df[df[cluster_column] == cluster_id]
            cluster_lsi_data = cluster_df['cluster_lsi_phrases'].iloc[0] if len(cluster_df) > 0 else []
            
            if cluster_id == -1:
                # Запросы без кластера
                queries_without_cluster += len(cluster_df)
            elif not isinstance(cluster_lsi_data, list) or len(cluster_lsi_data) == 0:
                clusters_without_lsi.append(cluster_id)
                queries_with_cluster_but_no_lsi += len(cluster_df)
        
        if queries_without_cluster > 0:
            print(f"ℹ️  Запросов без кластера (semantic_cluster_id = -1): {queries_without_cluster}")
        
        if clusters_without_lsi:
            print(f"⚠️  Найдено {len(clusters_without_lsi)} кластеров без LSI фраз после агрегации")
            print(f"   Запросов в таких кластерах: {queries_with_cluster_but_no_lsi}")
            if len(clusters_without_lsi) <= 20:
                print(f"   ID кластеров: {clusters_without_lsi[:20]}")
        
        print(f"📊 Статистика LSI фраз кластеров:")
        print(f"   Записей с маппингом: {mapped_count}/{len(df)}")
        print(f"   Записей с непустыми LSI: {non_empty_count}/{len(df)}")
        print(f"   Кластеров без LSI: {len(clusters_without_lsi)}")
        if queries_without_cluster > 0 or queries_with_cluster_but_no_lsi > 0:
            print(f"   Запросов без LSI кластера: {queries_without_cluster + queries_with_cluster_but_no_lsi}")
        
        # Строковая версия для CSV
        df['cluster_lsi_phrases_str'] = df[cluster_column].map(cluster_to_lsi_str)
        df['cluster_lsi_phrases_str'] = df['cluster_lsi_phrases_str'].fillna('')
        
        # Также сохраняем полный список (все фразы, не только топ-30)
        df['cluster_lsi_full'] = df[cluster_column].map(
            lambda x: cluster_lsi.get(x, [])
        )
        
        return df
    
    def aggregate_cluster_serp_urls(
        self,
        df: pd.DataFrame,
        cluster_column: str = 'semantic_cluster_id',
        serp_urls_column: str = 'serp_urls',
        top_n: int = 10
    ) -> Dict[int, List[str]]:
        """
        Агрегировать общие SERP URL для каждого кластера
        
        Args:
            df: DataFrame с колонками [cluster_column, serp_urls_column]
            cluster_column: Название колонки с ID кластера
            serp_urls_column: Название колонки с SERP URL
            top_n: Количество топ URL на кластер
            
        Returns:
            Dict {cluster_id: [top_common_urls]}
        """
        from collections import Counter
        
        if cluster_column not in df.columns:
            print(f"⚠️  Колонка '{cluster_column}' не найдена")
            return {}
        
        if serp_urls_column not in df.columns:
            print(f"⚠️  Колонка '{serp_urls_column}' не найдена")
            return {}
        
        cluster_urls = {}
        
        # Группируем по кластерам
        for cluster_id, group in df.groupby(cluster_column):
            # Собираем все URL из всех запросов кластера
            url_counter = Counter()
            
            for urls_list in group[serp_urls_column]:
                if isinstance(urls_list, list):
                    # Если это список словарей - извлекаем URL
                    urls = []
                    for item in urls_list:
                        if isinstance(item, dict):
                            url = item.get('url', '')
                            if url:
                                urls.append(url)
                        elif isinstance(item, str):
                            if item:
                                urls.append(item)
                    url_counter.update(urls)
            
            # Топ URL по частоте встречаемости
            if url_counter:
                top_urls = [url for url, count in url_counter.most_common(top_n)]
                cluster_urls[cluster_id] = top_urls
            else:
                cluster_urls[cluster_id] = []
        
        return cluster_urls
    
    def add_cluster_serp_urls_to_dataframe(
        self,
        df: pd.DataFrame,
        cluster_urls: Dict[int, List[str]],
        cluster_column: str = 'semantic_cluster_id'
    ) -> pd.DataFrame:
        """
        Добавить агрегированные SERP URL в DataFrame
        
        Args:
            df: DataFrame
            cluster_urls: Агрегированные URL по кластерам
            cluster_column: Колонка с ID кластера
            
        Returns:
            DataFrame с новой колонкой 'cluster_common_urls'
        """
        # Создаем маппинг cluster_id -> URL string
        cluster_to_urls = {}
        for cluster_id, urls_list in cluster_urls.items():
            # Формируем строку с топ URL
            cluster_to_urls[cluster_id] = ', '.join(urls_list)
        
        # Добавляем колонку с общими URL кластера (не перезаписываем индивидуальные!)
        df['cluster_common_urls'] = df[cluster_column].map(cluster_to_urls)
        df['cluster_common_urls'] = df['cluster_common_urls'].fillna('')
        
        return df
    
    def export_cluster_lsi(self, cluster_lsi: Dict[int, List[Dict[str, Any]]]) -> str:
        """
        Экспортировать LSI фразы в строку
        
        Args:
            cluster_lsi: Агрегированные LSI по кластерам
            
        Returns:
            Строковое представление LSI фраз
        """
        result_lines = []
        for cluster_id, lsi_list in sorted(cluster_lsi.items()):
            phrases = []
            for item in lsi_list[:30]:
                if isinstance(item, dict):
                    phrase = item.get('phrase', '')
                    if phrase:
                        phrases.append(phrase)
                elif isinstance(item, str):
                    if item:
                        phrases.append(item)
            
            if phrases:
                result_lines.append(f"Cluster {cluster_id}: {', '.join(phrases)}")
        
        return '\n'.join(result_lines)
    
    def get_cluster_keywords_for_content(self, cluster_lsi: Dict[int, List[Dict[str, Any]]]) -> Dict[int, str]:
        """
        Получить ключевые слова для контента
        
        Args:
            cluster_lsi: Агрегированные LSI по кластерам
            
        Returns:
            Dict {cluster_id: keywords_string}
        """
        result = {}
        for cluster_id, lsi_list in cluster_lsi.items():
            phrases = []
            for item in lsi_list[:10]:  # Топ-10 для контента
                if isinstance(item, dict):
                    phrase = item.get('phrase', '')
                    if phrase:
                        phrases.append(phrase)
                elif isinstance(item, str):
                    if item:
                        phrases.append(item)
            
            result[cluster_id] = ', '.join(phrases) if phrases else ''
        
        return result
    
    def get_statistics(self, cluster_lsi: Dict[int, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Получить статистику по агрегации
        
        Args:
            cluster_lsi: Агрегированные LSI по кластерам
            
        Returns:
            Словарь со статистикой
        """
        total_clusters = len(cluster_lsi)
        clusters_with_lsi = sum(1 for lsi_list in cluster_lsi.values() if lsi_list)
        clusters_without_lsi = total_clusters - clusters_with_lsi
        
        total_phrases = sum(len(lsi_list) for lsi_list in cluster_lsi.values())
        avg_phrases_per_cluster = total_phrases / total_clusters if total_clusters > 0 else 0
        
        return {
            'total_clusters': total_clusters,
            'clusters_with_lsi': clusters_with_lsi,
            'clusters_without_lsi': clusters_without_lsi,
            'total_phrases': total_phrases,
            'total_unique_phrases': total_phrases,  # Для совместимости с кодом
            'avg_phrases_per_cluster': round(avg_phrases_per_cluster, 2)
        }


__all__ = ['ClusterLSIAggregator']
