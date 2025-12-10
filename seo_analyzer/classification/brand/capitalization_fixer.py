"""
Smart Capitalization Fixer
Восстанавливает капитализацию в поисковых запросах для улучшения NER

Использует Natasha для ДИНАМИЧЕСКОГО определения потенциальных брендов (БЕЗ хардкода!)
"""

import re
from typing import Set
from natasha import (
    Segmenter,
    NewsEmbedding,
    NewsMorphTagger,
    Doc
)


class CapitalizationFixer:
    """
    Восстанавливает капитализацию в запросах для NER.
    
    Стратегия:
    1. Капитализируем первое слово (стандарт для предложений)
    2. Капитализируем слова, похожие на бренды (латиница, CamelCase)
    3. Капитализируем известные акронимы (СКУД, ВТБ, и т.д.)
    4. Используем словарь известных брендов для точной капитализации
    """
    
    def __init__(self, known_brands: Set[str] = None):
        """
        Args:
            known_brands: Набор известных брендов с правильной капитализацией
                         Например: {"Samsung", "Болид", "Hikvision"}
        """
        self.known_brands = known_brands or set()
        
        # Словарь известных брендов в lowercase → правильная капитализация
        self.brand_map = {b.lower(): b for b in self.known_brands}
        
        # Инициализация Natasha для морфологического анализа
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
    
    def fix_capitalization(self, query: str) -> str:
        """
        Восстанавливает капитализацию в запросе через Natasha (БЕЗ хардкода!).
        
        Использует морфологический анализ для определения:
        - NOUN (собственные) → Capitalize (потенциальные бренды)
        - VERB, ADJ, ADP, SCONJ → lowercase (не бренды)
        - Латиница → Capitalize (почти всегда бренды)
        - Короткие слова UPPERCASE → акронимы (СКУД, ВТБ)
        
        Args:
            query: Запрос в lowercase
            
        Returns:
            Запрос с восстановленной капитализацией
            
        Example:
            >>> fixer = CapitalizationFixer({"Samsung", "Болид"})
            >>> fixer.fix_capitalization("купить скуд болид")
            'купить СКУД Болид'
        """
        if not query.strip():
            return query
        
        # Анализируем через Natasha
        doc = Doc(query.lower())
        doc.segment(self.segmenter)
        doc.tag_morph(self.morph_tagger)
        
        # Создаем словарь: текст токена → POS
        token_pos = {}
        for token in doc.tokens:
            if token.text and token.pos:
                token_pos[token.text] = token.pos
        
        words = query.split()
        fixed_words = []
        
        for word in words:
            word_lower = word.lower()
            
            # 1. Известный бренд из словаря
            if word_lower in self.brand_map:
                fixed_words.append(self.brand_map[word_lower])
            
            # 2. Латинское слово (почти всегда бренд) → Capitalize
            elif self._is_latin(word_lower):
                fixed_words.append(self._smart_capitalize_latin(word_lower))
            
            # 3. Короткое слово (2-4 символов) → проверяем на акроним
            elif len(word_lower) >= 2 and len(word_lower) <= 4 and word_lower in token_pos:
                # Если это NOUN и все буквы согласные - вероятно акроним (СКУД, ВТБ, МЧС)
                if token_pos[word_lower] == 'NOUN' and self._is_likely_acronym(word_lower):
                    fixed_words.append(word_lower.upper())
                else:
                    fixed_words.append(word_lower)
            
            # 4. Используем POS-теги от Natasha
            elif word_lower in token_pos:
                pos = token_pos[word_lower]
                
                # NOUN (существительное) → может быть брендом
                if pos == 'NOUN':
                    # Проверяем, не является ли нарицательным
                    if self._is_proper_noun_candidate(word_lower):
                        fixed_words.append(word_lower.capitalize())
                    else:
                        fixed_words.append(word_lower)
                
                # PROPN (собственное имя) → точно бренд
                elif pos == 'PROPN':
                    fixed_words.append(word_lower.capitalize())
                
                # VERB, ADJ, ADP, SCONJ и т.д. → не бренды
                else:
                    fixed_words.append(word_lower)
            
            # 5. Fallback - оставляем lowercase
            else:
                fixed_words.append(word_lower)
        
        return ' '.join(fixed_words)
    
    def _is_latin(self, word: str) -> bool:
        """Проверяет, является ли слово латиницей"""
        return bool(re.match(r'^[a-z0-9\-]+$', word, re.IGNORECASE))
    
    def _smart_capitalize_latin(self, word: str) -> str:
        """
        Умная капитализация латиницы.
        
        Правила:
        - Если есть дефис → capitalize каждую часть (hi-tech → Hi-Tech)
        - Если есть цифры в середине → capitalize начало (ds2cd → Ds2cd)
        - Иначе → просто capitalize
        """
        if '-' in word:
            # hi-tech → Hi-Tech
            parts = word.split('-')
            return '-'.join(p.capitalize() for p in parts)
        
        # Просто capitalize
        return word.capitalize()
    
    def _is_likely_acronym(self, word: str) -> bool:
        """
        Определяет, является ли слово акронимом.
        
        Акронимы обычно:
        - Короткие (2-4 символа)
        - Содержат много согласных
        - Не имеют типичных окончаний слов
        
        Args:
            word: Слово в lowercase
            
        Returns:
            True если слово похоже на акроним
            
        Example:
            >>> self._is_likely_acronym("скуд")  # True - 4 согласных
            >>> self._is_likely_acronym("втб")   # True - 3 согласных
            >>> self._is_likely_acronym("цена")  # False - есть гласные, не акроним
        """
        if len(word) < 2 or len(word) > 4:
            return False
        
        # Считаем гласные
        vowels = set('аеёиоуыэюя')
        vowel_count = sum(1 for char in word if char in vowels)
        
        # Если гласных <= 1 - вероятно акроним
        # СКУД (1 гласная), ВТБ (0 гласных), МЧС (0 гласных)
        return vowel_count <= 1
    
    def _is_proper_noun_candidate(self, word: str) -> bool:
        """
        Определяет, является ли существительное собственным (бренд/название).
        
        Используется эвристика БЕЗ хардкода:
        - Длина >= 5 символов (короткие слова обычно нарицательные)
        - Содержит необычные сочетания букв (редкие для русского языка)
        - Не заканчивается на типичные окончания нарицательных (-ция, -ние, -ство)
        
        Args:
            word: Слово в lowercase
            
        Returns:
            True если слово похоже на собственное существительное (бренд)
        """
        if len(word) < 5:
            return False
        
        # Типичные окончания нарицательных существительных
        # Собственные имена редко так заканчиваются
        common_endings = ('ция', 'ние', 'ство', 'ость', 'ение', 'ание', 'ение', 'тель')
        
        if word.endswith(common_endings):
            return False
        
        # Если слово длинное и не имеет типичных окончаний - вероятно бренд
        return True
    
    def add_known_brands(self, brands: Set[str]):
        """
        Добавляет известные бренды в словарь.
        
        Args:
            brands: Набор брендов с правильной капитализацией
        """
        self.known_brands.update(brands)
        self.brand_map.update({b.lower(): b for b in brands})
    
    def learn_from_query(self, original_query: str):
        """
        Обучается на оригинальном запросе с правильной капитализацией.
        
        Если в запросе есть капитализированные слова, запоминаем их как бренды.
        
        Args:
            original_query: Запрос с оригинальной капитализацией
        """
        words = original_query.split()
        
        for word in words:
            # Если слово капитализировано (не первое в предложении)
            if word and word[0].isupper() and len(word) > 2:
                word_lower = word.lower()
                
                # Если это не акроним и не в словаре
                if word_lower not in self.acronyms and word_lower not in self.brand_map:
                    # Добавляем как возможный бренд
                    self.brand_map[word_lower] = word
                    self.known_brands.add(word)

