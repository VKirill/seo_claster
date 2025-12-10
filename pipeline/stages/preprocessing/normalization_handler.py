"""
Обработка нормализации и лемматизации
"""

import asyncio
from seo_analyzer.core.normalizer import QueryNormalizer


class NormalizationHandler:
    """Обработчик нормализации"""
    
    def __init__(self):
        """Инициализация обработчика"""
        self.normalizer = QueryNormalizer()
    
    async def normalize_queries(self, queries_list, print_stage):
        """
        Нормализовать список запросов
        
        Args:
            queries_list: Список запросов
            print_stage: Функция для логирования
            
        Returns:
            Список результатов нормализации
        """
        print_stage("🔄 Нормализация запросов...")
        
        # Выполняем в отдельном потоке чтобы не блокировать event loop
        normalized_results = await asyncio.to_thread(
            self.normalizer.normalize_batch,
            queries_list
        )
        
        print_stage(f"✓ Нормализация завершена")
        return normalized_results
    
    def apply_normalization_to_df(self, df, normalized_results):
        """
        Применить результаты нормализации к DataFrame
        
        Args:
            df: DataFrame
            normalized_results: Результаты нормализации
            
        Returns:
            DataFrame с добавленными колонками
        """
        for key in ['normalized', 'lemmatized', 'word_count', 'has_latin', 'has_numbers']:
            df[key] = [r[key] for r in normalized_results]
        
        df.rename(columns={'word_count': 'words_count'}, inplace=True)
        return df

