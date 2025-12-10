"""Определение целевых страниц для запросов

Устаревший модуль. Используйте seo_analyzer.classification.page_mapping.mapper.PageMapper
"""

from .page_mapping.mapper import PageMapper
from .page_mapping.type_determiner import PageType

__all__ = ['PageMapper', 'PageType']

