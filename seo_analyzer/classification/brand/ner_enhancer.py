"""
NER Brand Enhancer
Дополнительная валидация брендов через Named Entity Recognition (Natasha)
"""

from typing import List, Set
from natasha import (
    Segmenter,
    NewsEmbedding,
    NewsNERTagger,
    ORG,
    Doc
)
from .capitalization_fixer import CapitalizationFixer


class NERBrandEnhancer:
    """
    Дополняет эвристическое определение брендов через NER.
    
    Использует Natasha для извлечения именованных сущностей типа ORG (организации),
    которые с высокой вероятностью являются брендами.
    
    Преимущества:
    - Автоматическое определение известных организаций
    - Меньше ложных срабатываний (только валидные ORG)
    - Дополнительная проверка для повышения confidence
    """
    
    def __init__(self, known_brands: Set[str] = None):
        """
        Инициализация NER компонентов Natasha
        
        Args:
            known_brands: Набор известных брендов с правильной капитализацией
                         Используется для восстановления регистра в lowercase запросах
        """
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()  # Navec эмбеддинги под капотом
        self.ner_tagger = NewsNERTagger(self.emb)
        self.enabled = True
        
        # Инициализируем fixer для восстановления капитализации
        self.cap_fixer = CapitalizationFixer(known_brands)
    
    def extract_organizations(self, query: str) -> List[str]:
        """
        Извлекает названия организаций (потенциальные бренды) из запроса.
        
        ВАЖНО: NER чувствителен к регистру! Использует оригинальный текст, НЕ lowercase.
        
        Args:
            query: Поисковый запрос (в оригинальном регистре!)
            
        Returns:
            Список названий организаций в нижнем регистре
            
        Example:
            >>> enhancer = NERBrandEnhancer()
            >>> enhancer.extract_organizations("купить Болид СКУД")
            ['болид', 'скуд']
            >>> enhancer.extract_organizations("система контроля доступа")
            []
        """
        if not self.enabled or not query.strip():
            return []
        
        try:
            # Восстанавливаем капитализацию если запрос в lowercase
            # NER чувствителен к регистру и работает лучше с правильной капитализацией
            if query.islower():
                query = self.cap_fixer.fix_capitalization(query)
            
            # Создаем Doc с правильным регистром (NER чувствителен к капитализации!)
            doc = Doc(query)
            doc.segment(self.segmenter)
            doc.tag_ner(self.ner_tagger)
            
            # Извлекаем только ORG (организации)
            organizations = []
            for span in doc.spans:
                if span.type == ORG:
                    # Нормализуем название (убираем лишние пробелы)
                    org_name = ' '.join(span.text.split()).lower()
                    
                    # Если это фраза из нескольких слов, разбиваем на отдельные слова
                    # Это нужно для правильного сравнения с эвристическими брендами
                    words = org_name.split()
                    if len(words) > 1:
                        # Добавляем и полную фразу, и отдельные слова
                        organizations.append(org_name)
                        organizations.extend(words)
                    else:
                        organizations.append(org_name)
            
            # Удаляем дубликаты
            return list(set(organizations))
            
        except Exception as e:
            # Если NER не сработал, возвращаем пустой список
            return []
    
    def is_organization(self, word: str, query_context: str = None) -> bool:
        """
        Проверяет, является ли слово организацией.
        
        Args:
            word: Слово для проверки
            query_context: Полный контекст запроса (опционально, для лучшей точности)
            
        Returns:
            True если слово - часть организации по NER
            
        Example:
            >>> enhancer = NERBrandEnhancer()
            >>> enhancer.is_organization("болид", "купить болид скуд")
            True
            >>> enhancer.is_organization("система", "система контроля")
            False
        """
        if not self.enabled:
            return False
        
        # Если есть контекст - используем его (с оригинальным регистром!)
        text_to_check = query_context if query_context else word.capitalize()  # Capitalize для одиночного слова
        
        organizations = self.extract_organizations(text_to_check)
        
        # Проверяем, содержится ли слово в каком-либо названии организации
        word_lower = word.lower()
        return any(word_lower in org or org in word_lower for org in organizations)
    
    def get_organization_spans(self, query: str) -> List[dict]:
        """
        Возвращает детальную информацию о найденных организациях.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Список словарей с информацией о каждой организации:
            {
                'text': 'болид',
                'start': 7,
                'stop': 12,
                'type': 'ORG'
            }
        """
        if not self.enabled or not query.strip():
            return []
        
        try:
            # НЕ lowercase - NER чувствителен к регистру!
            doc = Doc(query)
            doc.segment(self.segmenter)
            doc.tag_ner(self.ner_tagger)
            
            org_spans = []
            for span in doc.spans:
                if span.type == ORG:
                    org_spans.append({
                        'text': span.text,
                        'start': span.start,
                        'stop': span.stop,
                        'type': span.type
                    })
            
            return org_spans
            
        except Exception:
            return []
    
    def validate_brand_list(self, brands: List[str], query: str) -> List[dict]:
        """
        Валидирует список брендов через NER, добавляя метку подтверждения.
        
        Args:
            brands: Список брендов для проверки
            query: Исходный запрос для контекста
            
        Returns:
            Список словарей с информацией о валидации:
            {
                'brand': 'болид',
                'ner_validated': True,
                'ner_confidence_boost': 0.2
            }
        """
        organizations = self.extract_organizations(query)
        
        validated = []
        for brand in brands:
            brand_lower = brand.lower()
            
            # Проверяем, подтверждается ли бренд через NER
            is_validated = any(
                brand_lower in org or org in brand_lower 
                for org in organizations
            )
            
            validated.append({
                'brand': brand,
                'ner_validated': is_validated,
                'ner_confidence_boost': 0.2 if is_validated else 0.0
            })
        
        return validated

