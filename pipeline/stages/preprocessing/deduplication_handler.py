"""
Обработка дедупликации запросов
"""

from pathlib import Path
from seo_analyzer.clustering.deduplicator import QueryDeduplicator
from seo_analyzer.clustering.advanced_deduplicator import AdvancedDeduplicator, load_stopwords_from_file


class DeduplicationHandler:
    """Обработчик дедупликации"""
    
    def __init__(self):
        """Инициализация обработчика"""
        self.deduplicator = None
        self.advanced_deduplicator = None
        self.removed_implicit_duplicates = None
    
    def deduplicate_exact(self, df, print_stage):
        """
        Стандартная дедупликация (точные дубли)
        
        Args:
            df: DataFrame
            print_stage: Функция для логирования
            
        Returns:
            DataFrame без дублей
        """
        print_stage("🔄 Дедупликация (точные дубли)...")
        self.deduplicator = QueryDeduplicator()
        df = self.deduplicator.deduplicate(
            df,
            normalized_column='normalized',
            original_column='keyword',
            freq_column='frequency_exact'
        )
        
        stats = self.deduplicator.get_deduplication_stats()
        print_stage(f"✓ Удалено точных дублей: {stats['total_duplicates_removed']}")
        return df, stats
    
    def deduplicate_advanced(self, df, print_stage):
        """
        Продвинутая дедупликация (неявные дубли)
        
        Args:
            df: DataFrame
            print_stage: Функция для логирования
            
        Returns:
            DataFrame без дублей и статистика
        """
        print_stage("🔄 Поиск неявных дублей...")
        stopwords_file = Path('keywords_settings/stop_keywords.txt')
        dedup_stopwords = load_stopwords_from_file(stopwords_file) if stopwords_file.exists() else set()
        
        self.advanced_deduplicator = AdvancedDeduplicator(stopwords=dedup_stopwords)
        df, removed_df = self.advanced_deduplicator.remove_duplicates(
            df,
            keyword_column='keyword',
            freq_column='frequency_world'
        )
        
        adv_stats = self.advanced_deduplicator.get_deduplication_stats()
        print_stage(f"✓ Удалено неявных дублей: {adv_stats['total_duplicates_removed']} ({adv_stats['duplicate_groups']} групп)")
        print_stage(f"✓ Уникальных запросов: {adv_stats['unique_queries']}")
        
        self.removed_implicit_duplicates = removed_df
        return df, adv_stats

