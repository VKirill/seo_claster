"""
Извлечение именованных сущностей (NER) через Natasha.

Находит:
- PER (персоны): имена людей
- LOC (локации): города, страны
- ORG (организации): компании, бренды
"""
from typing import Dict, List
import re


class NERExtractor:
    """Извлекает именованные сущности из запросов"""
    
    def __init__(self):
        """Инициализация экстрактора"""
        try:
            from natasha import (
                Segmenter,
                NewsEmbedding,
                NewsNERTagger,
                Doc,
                PER, LOC, ORG
            )
            
            self.segmenter = Segmenter()
            self.emb = NewsEmbedding()
            self.ner_tagger = NewsNERTagger(self.emb)
            
            self.PER = PER
            self.LOC = LOC
            self.ORG = ORG
            
            self.enabled = True
        except Exception as e:
            print(f"⚠️ Natasha NER недоступна: {e}")
            self.enabled = False
    
    def extract_entities(self, query: str) -> Dict[str, any]:
        """
        Извлекает именованные сущности из запроса.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Словарь с сущностями:
            {
                'persons': ['Иван Иванов', ...],      # Персоны
                'locations': ['Москва', 'Россия'],    # Локации
                'organizations': ['Яндекс', 'Google'],  # Организации
                'all_entities': ['Москва', 'Яндекс'],   # Все вместе
                'has_entities': bool,  # Есть ли сущности
            }
        """
        if not self.enabled:
            return {
                'persons': [],
                'locations': [],
                'organizations': [],
                'all_entities': [],
                'has_entities': False
            }
        
        try:
            # Создаем документ
            doc = Doc(query)
            doc.segment(self.segmenter)
            doc.tag_ner(self.ner_tagger)
            
            # Извлекаем сущности по типам
            persons = []
            locations = []
            organizations = []
            
            for span in doc.spans:
                if span.type == self.PER:
                    persons.append(span.text)
                elif span.type == self.LOC:
                    locations.append(span.text)
                elif span.type == self.ORG:
                    organizations.append(span.text)
            
            # Все сущности вместе
            all_entities = persons + locations + organizations
            
            return {
                'persons': persons,
                'locations': locations,
                'organizations': organizations,
                'all_entities': all_entities,
                'has_entities': len(all_entities) > 0
            }
            
        except Exception as e:
            return {
                'persons': [],
                'locations': [],
                'organizations': [],
                'all_entities': [],
                'has_entities': False,
                'error': str(e)
            }
    
    def get_entities_string(self, query: str) -> str:
        """
        Возвращает все сущности в виде строки (для CSV колонки).
        
        Args:
            query: Запрос
            
        Returns:
            Строка с сущностями через запятую
        """
        result = self.extract_entities(query)
        if result['all_entities']:
            return ', '.join(result['all_entities'])
        return ''
    
    def get_locations_string(self, query: str) -> str:
        """
        Возвращает только локации в виде строки.
        
        Args:
            query: Запрос
            
        Returns:
            Строка с локациями через запятую
        """
        result = self.extract_entities(query)
        if result['locations']:
            return ', '.join(result['locations'])
        return ''
    
    def has_brand(self, query: str) -> bool:
        """
        Проверяет наличие брендов/организаций.
        
        Args:
            query: Запрос
            
        Returns:
            True если найдены организации
        """
        result = self.extract_entities(query)
        return len(result['organizations']) > 0
    
    def extract_batch(self, queries: List[str]) -> List[Dict]:
        """
        Извлекает сущности для списка запросов.
        
        Args:
            queries: Список запросов
            
        Returns:
            Список словарей с результатами
        """
        results = []
        
        for query in queries:
            result = self.extract_entities(query)
            result['query'] = query
            results.append(result)
        
        return results


