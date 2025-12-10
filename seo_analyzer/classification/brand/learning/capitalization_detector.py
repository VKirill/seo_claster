"""
Определение капитализации слов
"""

import sys
import importlib.util
from pathlib import Path
from typing import List, Set
from collections import Counter


class CapitalizationDetector:
    """Детектор капитализации"""
    
    @staticmethod
    def detect_capitalization(queries: List[str], cap_fixer=None) -> Dict[str, int]:
        """
        Определяет капитализацию слов в запросах
        
        Args:
            queries: Список запросов
            cap_fixer: Фиксатор капитализации
            
        Returns:
            Словарь слово (lowercase) → количество капитализированных встреч
        """
        capitalized_counter = Counter()
        
        for query in queries:
            words = query.split()
            for word in words:
                if len(word) <= 2:
                    continue
                
                word_lower = word.lower()
                
                # Проверяем капитализацию
                if word[0].isupper():
                    capitalized_counter[word_lower] += 1
                else:
                    # Для lowercase слов пробуем восстановить капитализацию
                    if cap_fixer and not CapitalizationDetector._is_latin(word):
                        fixed_word = cap_fixer.fix_capitalization(word)
                        if fixed_word != word and fixed_word[0].isupper():
                            capitalized_counter[word_lower] += 1
        
        return dict(capitalized_counter)
    
    @staticmethod
    def _is_latin(word: str) -> bool:
        """Проверяет, является ли слово латиницей"""
        import re
        return bool(re.match(r'^[a-z0-9\-]+$', word, re.IGNORECASE))

