"""Экспорт результатов в JSON"""

import json
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from .json import (
    ClusterInfoBuilder,
    export_commercial_clusters,
    export_informational_clusters
)


class JSONExporter:
    """Экспортер результатов в JSON с иерархической структурой"""
    
    def __init__(self, indent: int = 2, relationships: Dict = None):
        """
        Инициализация
        
        Args:
            indent: Отступ для форматирования
            relationships: Словарь связей между кластерами
        """
        self.indent = indent
        self.relationships = relationships or {}
        self.cluster_builder = ClusterInfoBuilder(relationships=self.relationships)
    
    def export_hierarchical(
        self,
        df: pd.DataFrame,
        output_path: Path,
        cluster_column: str = 'semantic_cluster_id',
        clustering_params: Dict = None
    ) -> bool:
        """
        Экспортирует результаты в иерархическом JSON
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            cluster_column: Колонка кластера
            clustering_params: Параметры кластеризации для сохранения
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт иерархического JSON: {output_path.name}...")
            
            # Строим иерархическую структуру
            hierarchy = {
                'main_cluster': 'СКУД',
                'total_queries': len(df),
                'total_frequency': int(pd.to_numeric(df['frequency_world'], errors='coerce').sum()) if 'frequency_world' in df.columns else 0,
                'subclusters': []
            }
            
            # Добавляем параметры кластеризации если указаны
            if clustering_params:
                hierarchy['clustering_params'] = clustering_params
            
            # Группируем по кластерам
            if cluster_column in df.columns:
                for cluster_id, cluster_df in df.groupby(cluster_column):
                    cluster_info = self.cluster_builder.build(cluster_id, cluster_df)
                    hierarchy['subclusters'].append(cluster_info)
            
            # Сохраняем
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(hierarchy, f, ensure_ascii=False, indent=self.indent)
            
            print(f"✓ Экспортировано в JSON: {len(hierarchy['subclusters'])} кластеров")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта JSON: {e}")
            return False
    
    def export_commercial_clusters(
        self,
        df: pd.DataFrame,
        output_path: Path,
        cluster_column: str = 'semantic_cluster_id'
    ) -> bool:
        """
        Экспортирует только полностью коммерческие кластеры
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            cluster_column: Колонка кластера
            
        Returns:
            True если успешно
        """
        return export_commercial_clusters(
            df,
            output_path,
            self.cluster_builder.build,
            cluster_column,
            self.indent
        )
    
    def export_informational_clusters(
        self,
        df: pd.DataFrame,
        output_path: Path,
        cluster_column: str = 'semantic_cluster_id'
    ) -> bool:
        """
        Экспортирует только полностью информационные кластеры
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            cluster_column: Колонка кластера
            
        Returns:
            True если успешно
        """
        return export_informational_clusters(
            df,
            output_path,
            self.cluster_builder.build,
            cluster_column,
            self.indent
        )
    
    def export_flat(
        self,
        df: pd.DataFrame,
        output_path: Path
    ) -> bool:
        """
        Экспортирует в плоский JSON (массив объектов)
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт плоского JSON: {output_path.name}...")
            
            # Конвертируем DataFrame в список словарей
            records = df.to_dict('records')
            
            # Очищаем от NaN и конвертируем типы
            cleaned_records = []
            for record in records:
                cleaned = {}
                for k, v in record.items():
                    if pd.isna(v):
                        continue
                    # Конвертируем numpy типы в native Python типы
                    if isinstance(v, (np.integer, np.floating)):
                        cleaned[k] = float(v) if isinstance(v, np.floating) else int(v)
                    elif isinstance(v, np.bool_):
                        cleaned[k] = bool(v)
                    else:
                        cleaned[k] = v
                cleaned_records.append(cleaned)
            
            # Сохраняем
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_records, f, ensure_ascii=False, indent=self.indent)
            
            print(f"✓ Экспортировано {len(cleaned_records)} запросов")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта плоского JSON: {e}")
            return False
    
    def export_statistics(
        self,
        df: pd.DataFrame,
        output_path: Path,
        additional_stats: Dict = None
    ) -> bool:
        """
        Экспортирует статистику анализа
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            additional_stats: Дополнительная статистика
            
        Returns:
            True если успешно
        """
        try:
            print(f"💾 Экспорт статистики: {output_path.name}...")
            
            stats = {
                'total_queries': len(df),
                'total_frequency': int(pd.to_numeric(df['frequency_world'], errors='coerce').sum()) if 'frequency_world' in df.columns else 0,
            }
            
            # Распределение по интентам
            if 'main_intent' in df.columns:
                intent_dist = df['main_intent'].value_counts().to_dict()
                stats['intent_distribution'] = {str(k): int(v) for k, v in intent_dist.items()}
            
            # Распределение по воронке
            if 'funnel_stage' in df.columns:
                funnel_dist = df['funnel_stage'].value_counts().to_dict()
                stats['funnel_distribution'] = {str(k): int(v) for k, v in funnel_dist.items()}
            
            # Распределение по сложности
            if 'difficulty_cluster' in df.columns:
                diff_dist = df['difficulty_cluster'].value_counts().to_dict()
                stats['difficulty_distribution'] = {str(k): int(v) for k, v in diff_dist.items()}
            
            # Количество кластеров
            if 'semantic_cluster_id' in df.columns:
                stats['n_semantic_clusters'] = int(df['semantic_cluster_id'].nunique())
            
            if 'topic_id' in df.columns:
                stats['n_topics'] = int(df['topic_id'].nunique())
            
            # Добавляем дополнительную статистику
            if additional_stats:
                stats.update(additional_stats)
            
            # Сохраняем
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=self.indent)
            
            print(f"✓ Статистика экспортирована")
            return True
            
        except Exception as e:
            print(f"⚠️ Ошибка экспорта статистики: {e}")
            return False
