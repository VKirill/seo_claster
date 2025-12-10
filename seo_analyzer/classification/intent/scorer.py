"""Расчет скоров для классификации интента"""

from typing import Dict, Set
import re


def compile_word_pattern(words: Set[str]) -> re.Pattern:
    """
    Компилирует set слов в один regex паттерн
    
    Args:
        words: Множество слов
        
    Returns:
        Скомпилированный regex паттерн или None
    """
    if not words:
        return None
    # Сортируем по длине (от длинных к коротким) для корректного матчинга
    escaped_words = [re.escape(str(word)) for word in sorted(words, key=len, reverse=True)]
    pattern = r'\b(' + '|'.join(escaped_words) + r')\b'
    return re.compile(pattern, re.IGNORECASE)


def calculate_intent_scores(
    query: str,
    query_lower: str,
    commercial_pattern: re.Pattern,
    info_pattern: re.Pattern,
    info_patterns_compiled: re.Pattern,
    navigational_pattern: re.Pattern,
    intent_weights: Dict[str, float] = None
) -> Dict[str, float]:
    """
    Вычисляет скоры интента с предкомпилированными паттернами
    
    Args:
        query: Оригинальный запрос
        query_lower: Запрос в нижнем регистре
        commercial_pattern: Компилированный паттерн коммерческих слов из commercial.txt
        info_pattern: Компилированный паттерн информационных слов из info.txt
        info_patterns_compiled: Компилированный паттерн информационных фраз
        navigational_pattern: Компилированный паттерн навигационных слов
        intent_weights: Словарь весов для каждого типа интента из intent_weights.txt
        
    Returns:
        Словарь со скорами
    """
    # Веса по умолчанию (если не переданы)
    if intent_weights is None:
        intent_weights = {
            "commercial": 3.0,
            "informational": 4.0,
            "navigational": 4.0,
        }
    
    scores = {
        'commercial': 0.0,
        'informational': 0.0,
        'navigational': 0.0,
    }
    
    # Коммерческий скор - подсчитываем все совпадения из commercial.txt
    if commercial_pattern:
        matches = commercial_pattern.findall(query_lower)
        scores['commercial'] += len(matches) * intent_weights.get('commercial', 3.0)
    
    # Информационный скор из info.txt
    if info_pattern:
        matches = info_pattern.findall(query_lower)
        scores['informational'] += len(matches) * intent_weights.get('informational', 4.0)
    
    if info_patterns_compiled:
        matches = info_patterns_compiled.findall(query_lower)
        # Для фразовых паттернов используем тот же вес
        scores['informational'] += len(matches) * intent_weights.get('informational', 4.0) * 0.75
    
    # Навигационный скор
    if navigational_pattern:
        matches = navigational_pattern.findall(query_lower)
        scores['navigational'] += len(matches) * intent_weights.get('navigational', 4.0)
    
    return scores


def generate_flags(
    query_lower: str,
    keyword_dicts: Dict,
    commercial_words: Set[str],
    navigational_words: Set[str],
    has_geo_func
) -> Dict[str, bool]:
    """
    Генерирует булевы флаги для запроса на основе файлов из keyword_group
    
    Args:
        query_lower: Запрос в нижнем регистре
        keyword_dicts: Словари ключевых слов из keyword_group
        commercial_words: Коммерческие слова из commercial.txt
        navigational_words: Навигационные слова
        has_geo_func: Функция проверки гео
        
    Returns:
        Словарь с флагами
    """
    flags = {}
    
    # Проходим по всем словарям из keyword_group
    for dict_name, dict_data in keyword_dicts.items():
        flag_name = dict_data.get('flag')
        words = dict_data.get('words', set())
        
        if flag_name:
            flags[flag_name] = any(word in query_lower for word in words)
    
    # Дополнительные флаги
    flags['is_commercial'] = any(word in query_lower for word in commercial_words)
    flags['is_navigational'] = any(word in query_lower for word in navigational_words)
    
    # Географические флаги
    flags['has_geo'] = has_geo_func(query_lower)
    
    return flags

