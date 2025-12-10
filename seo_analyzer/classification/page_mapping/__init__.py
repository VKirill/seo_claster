"""
Page Mapping Module
Модули для определения целевых страниц для запросов
"""

from .mapper import PageMapper
from .type_determiner import PageTypeDeterminer, PageType

__all__ = ['PageMapper', 'PageTypeDeterminer', 'PageType']

