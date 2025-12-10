"""Построитель информации о кластере для JSON"""

import json
from typing import Dict
import pandas as pd
from .keyword_builder import KeywordInfoBuilder
from .price_info_builder import PriceInfoBuilder
from .direct_info_builder import DirectInfoBuilder
from .url_clustering_builder import URLClusteringBuilder


class ClusterInfoBuilder:
    """Построитель информации о кластере"""
    
    def __init__(self, relationships: Dict = None):
        """
        Инициализация билдеров
        
        Args:
            relationships: Словарь связей между кластерами (опционально)
        """
        self.keyword_builder = KeywordInfoBuilder()
        self.price_builder = PriceInfoBuilder()
        self.direct_builder = DirectInfoBuilder()
        self.url_builder = URLClusteringBuilder()
        self.relationships = relationships or {}
    
    def build(self, cluster_id: int, cluster_df: pd.DataFrame) -> Dict:
        """
        Строит информацию о кластере
        
        Args:
            cluster_id: ID кластера
            cluster_df: DataFrame кластера
            
        Returns:
            Словарь с информацией (важные поля в начале!)
        """
        cluster_info = {}
        
        # 1. Название кластера
        self._add_name(cluster_info, cluster_df)
        
        # 2. URL
        self._add_url(cluster_info, cluster_df)
        
        # 3. Частотность
        self._add_frequency(cluster_info, cluster_df)
        
        # 4. Воронка
        self._add_funnel(cluster_info, cluster_df)
        
        # 5. Интент
        self._add_intent(cluster_info, cluster_df)
        
        # 6. География кластера
        self._add_geo(cluster_info, cluster_df)
        
        # 7. ЦЕНЫ (для коммерческих запросов)
        self.price_builder.add(cluster_info, cluster_df)
        
        # 8. LSI фразы кластера
        self._add_lsi_phrases(cluster_info, cluster_df)
        
        # 9. YANDEX DIRECT ДАННЫЕ
        self.direct_builder.add(cluster_info, cluster_df)
        
        # 10. ID кластера и количество
        cluster_info['cluster_id'] = int(cluster_id) if not pd.isna(cluster_id) else -1
        cluster_info['keywords_count'] = len(cluster_df)
        
        # 11. Добавляем информацию о URL (почему сгруппировались)
        self.url_builder.add(cluster_info, cluster_df)
        
        # 12. Связанные кластеры (для перелинковки)
        self._add_related_clusters(cluster_info, cluster_id)
        
        # 13. Запросы (в самом конце)
        cluster_info['keywords'] = []
        for _, row in cluster_df.iterrows():
            keyword_info = self.keyword_builder.build(row)
            cluster_info['keywords'].append(keyword_info)
        
        return cluster_info
    
    def _add_name(self, cluster_info: Dict, cluster_df: pd.DataFrame):
        """Добавляет название кластера"""
        if 'cluster_name' in cluster_df.columns:
            cluster_name = cluster_df['cluster_name'].iloc[0]
            cluster_info['name'] = str(cluster_name) if not pd.isna(cluster_name) else ''
    
    def _add_url(self, cluster_info: Dict, cluster_df: pd.DataFrame):
        """Добавляет suggested URL"""
        if 'suggested_url' in cluster_df.columns:
            url = cluster_df['suggested_url'].iloc[0]
            cluster_info['target_url'] = str(url) if not pd.isna(url) else ''
    
    def _add_frequency(self, cluster_info: Dict, cluster_df: pd.DataFrame):
        """Добавляет частотность"""
        if 'frequency_world' in cluster_df.columns:
            freq_world = pd.to_numeric(cluster_df['frequency_world'], errors='coerce')
            cluster_info['total_frequency'] = int(freq_world.sum())
            cluster_info['avg_frequency'] = int(freq_world.mean())
        
        if 'frequency_exact' in cluster_df.columns:
            freq_exact = pd.to_numeric(cluster_df['frequency_exact'], errors='coerce')
            cluster_info['total_frequency_exact'] = int(freq_exact.sum())
            cluster_info['avg_frequency_exact'] = int(freq_exact.mean())
    
    def _add_funnel(self, cluster_info: Dict, cluster_df: pd.DataFrame):
        """Добавляет воронку"""
        if 'funnel_stage' in cluster_df.columns:
            funnel = cluster_df['funnel_stage'].mode()
            if len(funnel) > 0:
                cluster_info['funnel_stage'] = str(funnel.iloc[0])
    
    def _add_intent(self, cluster_info: Dict, cluster_df: pd.DataFrame):
        """Добавляет интент"""
        if 'main_intent' in cluster_df.columns:
            intent = cluster_df['main_intent'].mode()
            if len(intent) > 0:
                cluster_info['main_intent'] = str(intent.iloc[0])
    
    def _add_geo(self, cluster_info: Dict, cluster_df: pd.DataFrame):
        """Добавляет географию"""
        if 'cluster_geo_label' in cluster_df.columns:
            geo_label = cluster_df['cluster_geo_label'].iloc[0]
            if pd.notna(geo_label):
                cluster_info['geo'] = geo_label
    
    def _add_lsi_phrases(self, cluster_info: Dict, df: pd.DataFrame):
        """Добавляет LSI фразы кластера"""
        if 'cluster_lsi_phrases' not in df.columns:
            return
        
        lsi_data = df['cluster_lsi_phrases'].iloc[0]
        
        if not lsi_data or lsi_data == '':
            return
        
        # LSI фразы уже в формате списка словарей
        if isinstance(lsi_data, list):
            cluster_info['lsi_phrases'] = lsi_data
        elif isinstance(lsi_data, str):
            try:
                cluster_info['lsi_phrases'] = json.loads(lsi_data)
            except:
                pass
    
    def _add_related_clusters(self, cluster_info: Dict, cluster_id: int):
        """Добавляет связанные кластеры для перелинковки"""
        if not self.relationships or cluster_id not in self.relationships:
            cluster_info['related_clusters'] = []
            return
        
        # Получаем список связанных кластеров с детальной информацией
        related = []
        for rel_id, name, strength in self.relationships[cluster_id]:
            related.append({
                'cluster_id': rel_id,
                'cluster_name': name,
                'link_strength': strength
            })
        
        cluster_info['related_clusters'] = related

