"""
Brand Analyzer
Анализ слов на предмет определения брендов
"""

import re
from typing import Tuple
import pymorphy3


class BrandAnalyzer:
    """Анализатор слов для определения брендов"""
    
    def __init__(self, geo_terms: set, common_words: set):
        """
        Инициализация анализатора
        
        Args:
            geo_terms: Множество географических терминов для исключения
            common_words: Множество общих служебных слов
        """
        self.morph = pymorphy3.MorphAnalyzer()
        self.geo_terms = geo_terms
        self.common_words = common_words
    
    def is_likely_brand(self, word: str) -> Tuple[bool, float, str]:
        """Определяет, является ли слово брендом"""
        word_clean = word.strip()
        word_lower = word_clean.lower()
        
        # Исключаем слишком короткие слова
        if len(word_clean) < 2:
            return False, 0.0, "too_short"
        
        # Исключаем географию
        if word_lower in self.geo_terms:
            return False, 0.0, "geo_term"
        
        # Исключаем общие служебные слова
        if word_lower in self.common_words:
            return False, 0.0, "common_word"
        
        # Проверка 1: Латиница С ЗАГЛАВНОЙ
        if re.match(r'^[A-Z][a-zA-Z0-9\-]+$', word_clean):
            return True, 0.9, "latin_capitalized"
        
        # Проверка 1.5: ПОЛНОСТЬЮ латиница
        if re.match(r'^[a-z]+[a-z0-9\-]*$', word_clean) and any(c.isalpha() and c.isascii() for c in word_clean):
            if len(word_clean) >= 4:
                return True, 0.75, "latin_lowercase"
        
        # Проверка 2: CamelCase
        if re.match(r'^[A-Z][a-z]+[A-Z]', word_clean) or re.match(r'^i[A-Z][a-z]+', word_clean):
            return True, 0.85, "camel_case"
        
        # Проверка 2.5: Кириллица с ЗАГЛАВНОЙ
        if re.match(r'^[А-ЯЁ][а-яё]+$', word_clean):
            if len(word_clean) >= 4:
                lemma = self.morph.parse(word_clean)[0].normal_form
                if lemma.lower() not in self.common_words and lemma.lower() not in self.geo_terms:
                    return True, 0.70, "cyrillic_capitalized"
        
        # Проверка 3: Морфология - имена собственные
        parsed = self.morph.parse(word_clean)[0]
        
        if 'Name' in parsed.tag or 'Surn' in parsed.tag or 'Patr' in parsed.tag or 'Geox' in parsed.tag:
            if 'Geox' in parsed.tag and word_lower in self.geo_terms:
                return False, 0.0, "geo_morph"
            
            if 'Name' in parsed.tag or 'Surn' in parsed.tag:
                return True, 0.7, "morph_name"
        
        # Проверка 4: Латиница внутри кириллицы
        if re.search(r'[a-zA-Z]', word_clean) and re.search(r'[а-яА-ЯёЁ]', word_clean):
            return True, 0.6, "mixed_script"
        
        # Проверка 5: Заглавные буквы в середине
        if re.search(r'[а-яё][А-ЯЁ]', word_clean):
            return True, 0.65, "capitals_inside"
        
        # Проверка 6: Дефисы и цифры (модели)
        if re.match(r'^[A-ZА-ЯЁ][a-zA-Zа-яёЁ]*[\-\d]+', word_clean):
            return True, 0.75, "model_number"
        
        return False, 0.0, "none"







