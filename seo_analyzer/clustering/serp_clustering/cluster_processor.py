"""
Обработка кластеризации запросов
"""

import sys
import importlib.util
from pathlib import Path
import pandas as pd
import tempfile
import shutil


class ClusterProcessor:
    """Процессор кластеризации"""
    
    @staticmethod
    async def cluster_by_serp(
        df: pd.DataFrame,
        clusterer_instance,
        serp_column: str = 'serp_main_pages',
        geo_processor=None
    ) -> pd.DataFrame:
        """
        Выполняет продвинутую SOFT-кластеризацию запросов по SERP
        
        Args:
            df: DataFrame с запросами и SERP данными
            clusterer_instance: Экземпляр AdvancedSERPClusterer
            serp_column: Название колонки с SERP данными
            geo_processor: Процессор географии
            
        Returns:
            DataFrame с добавленными колонками semantic_cluster_id и cluster_name
        """
        # Если передан экземпляр кластеризатора, загружаем реализацию из backup файла
        if clusterer_instance:
            backup_path = Path(__file__).parent.parent / 'serp_advanced_clusterer.py.backup'
            if backup_path.exists():
                # Создаем временный файл с расширением .py для загрузки модуля
                # Python может не загрузить файл с расширением .backup
                temp_file = None
                try:
                    # Создаем временный файл с расширением .py
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
                        temp_file = Path(f.name)
                        # Читаем содержимое backup файла и исправляем относительные импорты
                        backup_content = backup_path.read_text(encoding='utf-8')
                        # Заменяем относительные импорты на абсолютные
                        backup_content = backup_content.replace(
                            'from .semantic_checker import',
                            'from seo_analyzer.clustering.semantic_checker import'
                        )
                        backup_content = backup_content.replace(
                            'from .fast_similarity import',
                            'from seo_analyzer.clustering.fast_similarity import'
                        )
                        # Записываем исправленное содержимое
                        temp_file.write_text(backup_content, encoding='utf-8')
                    
                    # Пытаемся загрузить временный файл как модуль
                    module_name = f"serp_advanced_clusterer_backup_{id(clusterer_instance)}"
                    spec = importlib.util.spec_from_file_location(module_name, str(temp_file))
                    
                    if spec is None or spec.loader is None:
                        # Не удалось загрузить модуль, используем fallback
                        df['semantic_cluster_id'] = -1
                        df['cluster_name'] = df['keyword']
                        return df
                    
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    # Создаем временный экземпляр с теми же параметрами
                    temp_instance = module.AdvancedSERPClusterer(
                        min_common_urls=clusterer_instance.min_common_urls,
                        top_positions=clusterer_instance.top_positions,
                        max_cluster_size=clusterer_instance.max_cluster_size,
                        mode=clusterer_instance.mode,
                        semantic_check=clusterer_instance.semantic_check,
                        min_cluster_cohesion=getattr(clusterer_instance, 'min_cluster_cohesion', 0.5)
                    )
                    # Копируем состояние
                    temp_instance.clusters = clusterer_instance.clusters
                    temp_instance.cluster_queries = clusterer_instance.cluster_queries
                    temp_instance.cluster_geo_cache = clusterer_instance.cluster_geo_cache
                    temp_instance.similarity_cache = clusterer_instance.similarity_cache
                    temp_instance.semantic_checker = clusterer_instance.semantic_checker
                    temp_instance.fast_similarity = clusterer_instance.fast_similarity
                    
                    # Вызываем метод кластеризации
                    result_df = await temp_instance.cluster_by_serp(df, serp_column, geo_processor)
                    
                    # Обновляем состояние обратно
                    clusterer_instance.clusters = temp_instance.clusters
                    clusterer_instance.cluster_queries = temp_instance.cluster_queries
                    clusterer_instance.cluster_geo_cache = temp_instance.cluster_geo_cache
                    clusterer_instance.similarity_cache = temp_instance.similarity_cache
                    
                    return result_df
                except Exception as e:
                    # Ошибка при загрузке модуля, используем fallback
                    print(f"⚠️  Ошибка при загрузке backup модуля: {e}")
                    df['semantic_cluster_id'] = -1
                    df['cluster_name'] = df['keyword']
                    return df
                finally:
                    # Удаляем временный файл
                    if temp_file and temp_file.exists():
                        try:
                            temp_file.unlink()
                        except Exception:
                            pass  # Игнорируем ошибки при удалении временного файла
            else:
                # Backup файл не найден, используем fallback
                df['semantic_cluster_id'] = -1
                df['cluster_name'] = df['keyword']
                return df
        
        # Fallback: если экземпляр не передан
        df['semantic_cluster_id'] = -1
        df['cluster_name'] = df['keyword']
        return df

