"""
Pattern Detector
Определение структурных паттернов запросов
Использует динамическую загрузку паттернов из keyword_group
"""

import re
from typing import Dict, Tuple, Set


class PatternDetector:
    """Детектор структурных паттернов запросов"""
    
    def __init__(self, commercial_words: Set[str] = None, info_words: Set[str] = None):
        """
        Инициализация детектора
        
        Args:
            commercial_words: Коммерческие слова из commercial.txt
            info_words: Информационные слова из info.txt
        """
        # Если слова не переданы, используем базовые паттерны
        self.commercial_words = commercial_words or set()
        self.info_words = info_words or set()
        
        # Создаем динамические паттерны на основе словарей
        commercial_pattern = '|'.join(re.escape(w.lower()) for w in list(self.commercial_words)[:20]) if self.commercial_words else 'купить|заказать'
        info_pattern = '|'.join(re.escape(w.lower()) for w in list(self.info_words)[:20]) if self.info_words else 'как|где|что'
        
        self.patterns = {
            'product': r'^[а-яё\s-]+$',
            'product_city': r'[а-яё\s-]+\s+[а-яё]+',  # Город определяется через geo_dicts
            'action_product': rf'^({commercial_pattern})\s+[а-яё\s-]+',
            'brand_product': r'[A-Z][a-zA-Z]+\s+[а-яё\s-]+',
            'question': rf'^({info_pattern})\s+',
            'product_modifier': r'[а-яё\s-]+\s+[а-яё]+',  # Модификаторы из словарей
            'product_price': r'[а-яё\s-]+\s+(цена|стоимость|прайс)',  # Оставляем базовые для цены
            'comparison': r'(или|vs|против|сравнение|отличи)',
        }
        
        self.pattern_names = {
            'product': '[продукт]',
            'product_city': '[продукт] + [город]',
            'action_product': '[действие] + [продукт]',
            'brand_product': '[бренд] + [продукт]',
            'question': '[вопрос] + [продукт]',
            'product_modifier': '[продукт] + [модификатор]',
            'product_price': '[продукт] + [цена]',
            'comparison': '[сравнение]',
        }
    
    def detect_pattern(self, query: str) -> Tuple[str, str]:
        """Определяет структурный паттерн запроса"""
        query_clean = query.strip().lower()
        
        priority_order = [
            'action_product', 'question', 'comparison', 'brand_product',
            'product_price', 'product_modifier', 'product_city', 'product'
        ]
        
        for pattern_key in priority_order:
            pattern = self.patterns[pattern_key]
            if re.search(pattern, query_clean):
                return pattern_key, self.pattern_names[pattern_key]
        
        return 'other', '[другое]'
    
    def analyze_structure(self, query: str) -> Dict[str, any]:
        """Детальный анализ структуры запроса"""
        pattern_key, pattern_name = self.detect_pattern(query)
        query_lower = query.lower()
        
        # Проверяем наличие слов из словарей
        has_action = any(word.lower() in query_lower for word in self.commercial_words) if self.commercial_words else False
        has_question = any(word.lower() in query_lower for word in self.info_words) if self.info_words else False
        
        return {
            'query_pattern': pattern_name,
            'pattern_key': pattern_key,
            'has_action': has_action,
            'has_question': has_question,
            'has_price': bool(re.search(r'цена|стоимость|прайс', query_lower)),
            'has_comparison': bool(re.search(r'или|vs|против|сравнение', query_lower)),
            'has_modifier': bool(re.search(r'дешево|дорого|качественн|лучш', query_lower)),
        }

