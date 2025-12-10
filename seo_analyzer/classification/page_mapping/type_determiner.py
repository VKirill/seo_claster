"""
Page Type Determiner
Определение типа целевой страницы по запросу
"""

from typing import Optional
from enum import Enum


class PageType(Enum):
    """Типы страниц"""
    MAIN = "main"
    CATEGORY = "category"
    SUBCATEGORY = "subcategory"
    PRODUCT = "product"
    COMMERCIAL = "commercial"
    GEO = "geo"
    ARTICLE = "article"
    BRAND = "brand"


class PageTypeDeterminer:
    """Определитель типа целевой страницы"""
    
    def __init__(self, commercial_words: set = None, info_words: set = None):
        """
        Инициализация определителя
        
        Args:
            commercial_words: Коммерческие слова из commercial.txt
            info_words: Информационные слова из info.txt
        """
        # Используем слова из файлов или базовые для обратной совместимости
        self.commercial_words = commercial_words or {'купить', 'заказать', 'цена'}
        self.info_words = info_words or {'что такое', 'как', 'почему'}
        
        self.page_patterns = {
            PageType.ARTICLE: list(self.info_words),
            PageType.COMMERCIAL: list(self.commercial_words),
            PageType.GEO: [],  # Гео определяется через geo_dicts
            PageType.BRAND: [],
            PageType.PRODUCT: ['модель', 'артикул'],
        }
    
    def determine(
        self,
        query: str,
        intent: str,
        has_brand: bool = False,
        has_geo: bool = False,
        funnel_stage: Optional[str] = None
    ) -> str:
        """Определяет тип целевой страницы"""
        query_lower = query.lower()
        
        # Брендовые запросы
        if has_brand:
            return PageType.BRAND.value
        
        # Информационные запросы
        if intent == 'informational' or intent == 'informational_geo':
            if has_geo or intent == 'informational_geo':
                # Информационные запросы с географией → статья с гео-контентом
                return PageType.ARTICLE.value
            for pattern in self.page_patterns[PageType.ARTICLE]:
                if pattern in query_lower:
                    return PageType.ARTICLE.value
            return PageType.ARTICLE.value
        
        # Коммерческие запросы
        if intent == 'commercial' or intent == 'commercial_geo':
            if has_geo or intent == 'commercial_geo':
                return PageType.GEO.value
            
            # Проверяем наличие коммерческих слов
            if any(word.lower() in query_lower for word in self.commercial_words):
                return PageType.COMMERCIAL.value
            
            return PageType.CATEGORY.value
        
        # Навигационные
        if intent == 'navigational':
            if has_brand:
                return PageType.BRAND.value
            return PageType.MAIN.value
        
        # По умолчанию
        return PageType.CATEGORY.value

