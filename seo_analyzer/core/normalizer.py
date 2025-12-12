"""Нормализация и лемматизация запросов"""

import re
from typing import List, Dict, Set
import pymorphy3
from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    Doc,
)


class QueryNormalizer:
    """Нормализатор запросов с использованием pymorphy3 и natasha"""
    
    def __init__(self):
        """Инициализация нормализатора"""
        # Pymorphy для лемматизации
        self.morph = pymorphy3.MorphAnalyzer()
        
        # Natasha для токенизации и морфологии
        self.segmenter = Segmenter()
        self.morph_vocab = MorphVocab()
        
        # Опционально: для более точной морфологии
        try:
            self.emb = NewsEmbedding()
            self.morph_tagger = NewsMorphTagger(self.emb)
            self.use_natasha_morph = True
        except:
            self.use_natasha_morph = False
            print("⚠️ Natasha морфология недоступна, используем только pymorphy3")
    
    def clean_text(self, text: str) -> str:
        """
        Очистка текста от лишних символов
        
        Args:
            text: Исходный текст
            
        Returns:
            Очищенный текст
        """
        # Приводим к нижнему регистру
        text = text.lower()
        
        # Убираем лишние пробелы
        text = re.sub(r'\s+', ' ', text)
        
        # Убираем спецсимволы, оставляем буквы, цифры, пробелы и дефисы
        text = re.sub(r'[^\w\s\-]', ' ', text)
        
        return text.strip()
    
    def tokenize(self, text: str) -> List[str]:
        """
        Токенизация текста
        
        Args:
            text: Текст
            
        Returns:
            Список токенов
        """
        text = self.clean_text(text)
        tokens = text.split()
        return [t for t in tokens if len(t) > 1]  # Фильтруем однобуквенные
    
    def lemmatize_word(self, word: str) -> str:
        """
        Лемматизация одного слова
        
        Args:
            word: Слово
            
        Returns:
            Лемма
        """
        parsed = self.morph.parse(word)[0]
        return parsed.normal_form
    
    def lemmatize(self, text: str) -> str:
        """
        Лемматизация всего текста
        
        Args:
            text: Текст
            
        Returns:
            Лемматизированный текст
        """
        tokens = self.tokenize(text)
        lemmas = [self.lemmatize_word(token) for token in tokens]
        return ' '.join(lemmas)
    
    def normalize_query(self, query: str) -> str:
        """
        Полная нормализация запроса:
        1. Очистка
        2. Лемматизация
        3. Сортировка слов (для дедупликации)
        
        Args:
            query: Исходный запрос
            
        Returns:
            Нормализованный запрос
        """
        lemmas = self.lemmatize(query).split()
        # Сортируем для единообразия
        lemmas.sort()
        return ' '.join(lemmas)
    
    def get_word_count(self, text: str) -> int:
        """
        Подсчет количества слов
        
        Args:
            text: Текст
            
        Returns:
            Количество слов
        """
        return len(self.tokenize(text))
    
    def extract_pos_tags(self, text: str) -> List[Dict[str, str]]:
        """
        Извлечение частей речи через natasha
        
        Args:
            text: Текст
            
        Returns:
            Список словарей с токенами и их POS-тегами
        """
        if not self.use_natasha_morph:
            # Fallback на pymorphy
            tokens = self.tokenize(text)
            result = []
            for token in tokens:
                parsed = self.morph.parse(token)[0]
                result.append({
                    'token': token,
                    'pos': parsed.tag.POS,
                    'lemma': parsed.normal_form,
                })
            return result
        
        # Используем natasha
        doc = Doc(text)
        doc.segment(self.segmenter)
        doc.tag_morph(self.morph_tagger)
        
        result = []
        for token in doc.tokens:
            result.append({
                'token': token.text,
                'pos': token.pos if hasattr(token, 'pos') else None,
                'lemma': token.lemma if hasattr(token, 'lemma') else token.text,
            })
        
        return result
    
    def has_latin(self, text: str) -> bool:
        """
        Проверяет наличие латиницы в тексте
        
        Args:
            text: Текст
            
        Returns:
            True если есть латиница
        """
        return bool(re.search(r'[a-zA-Z]', text))
    
    def has_numbers(self, text: str) -> bool:
        """
        Проверяет наличие цифр
        
        Args:
            text: Текст
            
        Returns:
            True если есть цифры
        """
        return bool(re.search(r'\d', text))
    
    def extract_numbers(self, text: str) -> List[str]:
        """
        Извлекает все числа из текста
        
        Args:
            text: Текст
            
        Returns:
            Список чисел
        """
        return re.findall(r'\d+', text)
    
    def normalize_batch(self, queries: List[str]) -> List[Dict[str, str]]:
        """
        Пакетная нормализация запросов
        
        Args:
            queries: Список запросов
            
        Returns:
            Список словарей с оригиналом и нормализацией
        """
        results = []
        
        for query in queries:
            normalized = self.normalize_query(query)
            lemmatized = self.lemmatize(query)
            
            results.append({
                'original': query,
                'normalized': normalized,
                'lemmatized': lemmatized,
                'word_count': self.get_word_count(query),
                'has_latin': self.has_latin(query),
                'has_numbers': self.has_numbers(query),
            })
        
        return results






