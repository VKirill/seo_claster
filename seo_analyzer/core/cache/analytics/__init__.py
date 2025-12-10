"""
Модули для аналитики Master DB
"""

from .statistics import DatabaseStatistics
from .index_stats import IndexStatistics

__all__ = ['DatabaseStatistics', 'IndexStatistics']

