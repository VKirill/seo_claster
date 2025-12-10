"""Классификация интента запросов (фасад для обратной совместимости)"""

from typing import Dict, List, Tuple
from enum import Enum

from .intent import IntentClassifier as IntentClassifierImpl, IntentType as IntentTypeImpl


class IntentType(Enum):
    """Типы интента"""
    COMMERCIAL = "commercial"
    COMMERCIAL_GEO = "commercial_geo"
    INFORMATIONAL = "informational"
    INFORMATIONAL_GEO = "informational_geo"
    NAVIGATIONAL = "navigational"
    TRANSACTIONAL = "transactional"
    BRAND = "brand"


class IntentClassifier:
    """
    Классификатор интента запросов
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.classification.intent
    """
    
    def __init__(self, keyword_dicts: Dict[str, Dict], geo_dicts: Dict[str, Set[str]], intent_weights: Dict[str, float] = None):
        self._classifier = IntentClassifierImpl(keyword_dicts, geo_dicts, intent_weights)
        # Делегируем атрибуты для обратной совместимости
        self.keyword_dicts = keyword_dicts
        self.geo_dicts = geo_dicts
        self.commercial_words = self._classifier.commercial_words
        self.info_words = self._classifier.info_words
        self.navigational_words = self._classifier.navigational_words
        self.info_patterns = self._classifier.info_patterns
    
    def classify_intent(self, query: str, lemmatized_query: str = None) -> Tuple[str, Dict[str, float], Dict[str, bool]]:
        """Классифицирует интент запроса"""
        return self._classifier.classify_intent(query, lemmatized_query)
    
    def classify_batch(self, queries: List[str], lemmatized_queries: List[str] = None) -> List[Dict[str, any]]:
        """Пакетная классификация"""
        return self._classifier.classify_batch(queries, lemmatized_queries)
    
    def extract_geo_info(self, query: str) -> Dict[str, any]:
        """Извлекает географическую информацию"""
        return self._classifier.extract_geo_info(query)

