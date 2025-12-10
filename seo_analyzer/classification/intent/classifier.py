"""Основной классификатор интента"""

from typing import Dict, List, Tuple, Set
from enum import Enum
from tqdm import tqdm

from .scorer import compile_word_pattern, calculate_intent_scores, generate_flags
from .geo_extractor import extract_geo_info, prepare_geo_patterns, has_geo


class IntentType(Enum):
    """Типы интента"""
    COMMERCIAL = "commercial"
    COMMERCIAL_GEO = "commercial_geo"  # Коммерческий с указанием города/региона
    INFORMATIONAL = "informational"
    INFORMATIONAL_GEO = "informational_geo"  # Информационный с указанием города/региона
    NAVIGATIONAL = "navigational"
    BRAND = "brand"


class IntentClassifier:
    """Классификатор интента запросов"""
    
    def __init__(
        self, 
        keyword_dicts: Dict[str, Dict], 
        geo_dicts: Dict[str, Set[str]],
        intent_weights: Dict[str, float] = None
    ):
        """
        Инициализация классификатора
        
        Args:
            keyword_dicts: Словари из keyword_group
            geo_dicts: Географические словари
            intent_weights: Веса для каждого типа интента (из файла)
        """
        self.keyword_dicts = keyword_dicts
        self.geo_dicts = geo_dicts
        self.intent_weights = intent_weights or {
            "commercial": 3.0,
            "informational": 4.0,
            "navigational": 4.0,
        }
        
        # Коммерческие маркеры из файла commercial.txt
        self.commercial_words = keyword_dicts.get('commercial', {}).get('words', set())
        
        # Информационные маркеры из файла info.txt
        self.info_words = keyword_dicts.get('info', {}).get('words', set())
        
        # Навигационные паттерны - TODO: вынести в отдельный файл navigation.txt
        self.navigational_words = {
            'сайт', 'официальный', 'официальный сайт', 'личный кабинет',
            'вход', 'регистрация', 'скачать', 'адрес', 'контакты'
        }
        
        # Информационные паттерны - берём из info.txt
        self.info_patterns = self.info_words
        
        # Предкомпилируем regex паттерны
        self._commercial_pattern = compile_word_pattern(self.commercial_words)
        self._info_pattern = compile_word_pattern(self.info_words)
        self._navigational_pattern = compile_word_pattern(self.navigational_words)
        self._info_patterns_compiled = compile_word_pattern(self.info_patterns)
        
        # Предкомпилируем географические паттерны
        self._compiled_geo_patterns, self._geo_city_map = prepare_geo_patterns(self.geo_dicts)
    
    def classify_intent(self, query: str, lemmatized_query: str = None) -> Tuple[str, Dict[str, float], Dict[str, bool]]:
        """
        Классифицирует интент запроса с приоритетом для гео-запросов
        
        Args:
            query: Оригинальный запрос
            lemmatized_query: Лемматизированный запрос (для точного поиска городов)
            
        Returns:
            Tuple (main_intent, scores, flags)
        """
        query_lower = query.lower()
        
        # Проверяем ГЕО (используем лемматизированный если есть)
        query_for_geo = lemmatized_query if lemmatized_query else query
        geo_info = extract_geo_info(
            query_for_geo,
            self._compiled_geo_patterns,
            self._geo_city_map
        )
        
        # Вычисляем скоры с учётом весов из файла
        scores = calculate_intent_scores(
            query,
            query_lower,
            self._commercial_pattern,
            self._info_pattern,
            self._info_patterns_compiled,
            self._navigational_pattern,
            self.intent_weights
        )
        
        # Определяем основной интент на основе содержания запроса
        max_score = max(scores.values())
        
        if max_score == 0:
            # Если нет явных маркеров - оставляем informational по умолчанию
            # Корректировка будет позже через SERP offer_info (если есть offer → commercial)
            base_intent = IntentType.INFORMATIONAL.value
            scores['informational'] = 1.0  # Минимальный базовый скор
        else:
            base_intent = max(scores.items(), key=lambda x: x[1])[0]
        
        # Если есть география, добавляем суффикс _geo к базовому интенту
        if geo_info['has_geo']:
            if base_intent == 'commercial':
                main_intent = IntentType.COMMERCIAL_GEO.value
            elif base_intent == 'informational':
                main_intent = IntentType.INFORMATIONAL_GEO.value
            else:
                # Для navigational и других типов оставляем как есть, но отмечаем гео
                main_intent = base_intent
        else:
            main_intent = base_intent
        
        # Генерируем флаги
        flags = self._generate_flags(query_lower)
        
        return main_intent, scores, flags
    
    def _generate_flags(self, query_lower: str) -> Dict[str, bool]:
        """Генерирует булевы флаги для запроса"""
        return generate_flags(
            query_lower,
            self.keyword_dicts,
            self.commercial_words,
            self.navigational_words,
            lambda q: has_geo(q, self.geo_dicts)
        )
    
    def extract_geo_info(self, query: str) -> Dict[str, any]:
        """Извлекает географическую информацию"""
        return extract_geo_info(
            query,
            self._compiled_geo_patterns,
            self._geo_city_map
        )
    
    def classify_batch(self, queries: List[str], lemmatized_queries: List[str] = None) -> List[Dict[str, any]]:
        """
        Пакетная классификация с прогресс-баром
        
        Args:
            queries: Список оригинальных запросов
            lemmatized_queries: Список лемматизированных запросов (опционально)
            
        Returns:
            Список результатов классификации
        """
        results = []
        
        # Показываем прогресс-бар только для больших датасетов
        show_progress = len(queries) >= 100
        iterator = tqdm(queries, desc="Классификация интента", disable=not show_progress)
        
        for idx, query in enumerate(iterator):
            # Для поиска гео используем лемматизированный запрос если доступен
            lemmatized = lemmatized_queries[idx] if lemmatized_queries else None
            
            main_intent, scores, flags = self.classify_intent(query, lemmatized)
            # geo_info уже извлечена внутри classify_intent, получаем её снова
            geo_info = self.extract_geo_info(lemmatized if lemmatized else query)
            
            result = {
                'query': query,
                'main_intent': main_intent,
                'commercial_score': scores.get('commercial', 0),
                'informational_score': scores.get('informational', 0),
                'navigational_score': scores.get('navigational', 0),
                **flags,
                **geo_info,
            }
            
            results.append(result)
        
        return results
