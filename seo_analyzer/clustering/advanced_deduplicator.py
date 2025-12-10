"""Продвинутая дедупликация (фасад для обратной совместимости)"""

from typing import Dict, List, Set, Tuple
import pandas as pd
from pathlib import Path

from .deduplication import AdvancedDeduplicator as AdvancedDeduplicatorImpl
from .deduplication.text_processor import load_stopwords_from_file as load_stopwords_from_file_impl


class AdvancedDeduplicator:
    """
    Дедупликатор для поиска неявных дублей с настраиваемыми правилами
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.clustering.deduplication
    """
    
    def __init__(self, stopwords: Set[str] = None):
        self._deduplicator = AdvancedDeduplicatorImpl(stopwords)
        # Делегируем атрибуты для обратной совместимости
        self.stopwords = stopwords or set()
        self.duplicates_count = self._deduplicator.duplicates_count
        self.unique_count = self._deduplicator.unique_count
        self.duplicate_groups = self._deduplicator.duplicate_groups
    
    def find_implicit_duplicates(self, df: pd.DataFrame, keyword_column: str = 'keyword') -> Dict[str, List[Dict]]:
        """Находит неявные дубли в DataFrame"""
        result = self._deduplicator.find_implicit_duplicates(df, keyword_column)
        self._sync_attributes()
        return result
    
    def remove_duplicates(self, df: pd.DataFrame, keyword_column: str = 'keyword', freq_column: str = 'frequency_world', keep_strategy: str = 'max_freq_random') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Удаляет дубли из DataFrame"""
        result = self._deduplicator.remove_duplicates(df, keyword_column, freq_column, keep_strategy)
        self._sync_attributes()
        return result
    
    def get_deduplication_stats(self) -> Dict[str, int]:
        """Возвращает статистику дедупликации"""
        return self._deduplicator.get_deduplication_stats()
    
    def export_duplicate_groups(self, output_path: Path, duplicate_groups: Dict[str, List[Dict]] = None):
        """Экспортирует группы дублей в CSV"""
        return self._deduplicator.export_duplicate_groups(output_path, duplicate_groups)
    
    def _sync_attributes(self):
        """Синхронизирует атрибуты для обратной совместимости"""
        self.duplicates_count = self._deduplicator.duplicates_count
        self.unique_count = self._deduplicator.unique_count
        self.duplicate_groups = self._deduplicator.duplicate_groups


def load_stopwords_from_file(filepath: Path) -> Set[str]:
    """Загружает стоп-слова из файла"""
    return load_stopwords_from_file_impl(filepath)

