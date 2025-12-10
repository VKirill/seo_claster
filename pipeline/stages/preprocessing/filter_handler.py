"""
Обработка фильтрации запросов
"""

from seo_analyzer.core.stopwords import StopwordsFilter, filter_short_queries, filter_low_frequency, filter_by_frequency_ratio


class FilterHandler:
    """Обработчик фильтрации запросов"""
    
    def __init__(self, stopwords):
        """
        Args:
            stopwords: Список стоп-слов
        """
        self.stopwords = stopwords
        self.stopwords_filter = None
    
    def apply_filters(self, df, args, print_stage):
        """
        Применить все фильтры к DataFrame
        
        Args:
            df: DataFrame с запросами
            args: Аргументы командной строки
            print_stage: Функция для логирования
            
        Returns:
            Отфильтрованный DataFrame
        """
        # Фильтрация стоп-слов
        self.stopwords_filter = StopwordsFilter(self.stopwords)
        df = self.stopwords_filter.filter_dataframe(df)
        print_stage(f"✓ Отфильтровано {self.stopwords_filter.blocked_count} запросов со стоп-словами")
        
        # Фильтрация запросов с низкой частотностью
        initial_count = len(df)
        min_freq = getattr(args, 'min_frequency', 1)
        df = filter_low_frequency(df, freq_column='frequency_world', min_freq=min_freq)
        low_freq_removed = initial_count - len(df)
        if low_freq_removed > 0:
            if min_freq == 1:
                print_stage(f"✓ Удалено {low_freq_removed} запросов с нулевой частотностью")
            else:
                print_stage(f"✓ Удалено {low_freq_removed} запросов с частотностью < {min_freq}")
        
        # Фильтрация по соотношению частотностей
        initial_count = len(df)
        max_ratio = getattr(args, 'max_frequency_ratio', 51.0)
        df = filter_by_frequency_ratio(
            df,
            freq_world_column='frequency_world',
            freq_exact_column='frequency_exact',
            max_ratio=max_ratio
        )
        ratio_removed = initial_count - len(df)
        if ratio_removed > 0:
            print_stage(f"✓ Удалено {ratio_removed} запросов с соотношением частотностей > {max_ratio}")
        
        # Фильтрация коротких запросов
        initial_count = len(df)
        df = filter_short_queries(df, min_length=2)
        print_stage(f"✓ Удалено {initial_count - len(df)} слишком коротких запросов")
        
        return df

