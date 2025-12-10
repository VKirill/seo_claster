"""Извлечение географической информации"""

from typing import Dict, Set, Any
import re


def prepare_geo_patterns(geo_dicts: Dict[str, Set[str]]) -> tuple[Dict[str, re.Pattern], Dict[str, str]]:
    """
    Предварительная компиляция regex паттернов для городов
    
    Args:
        geo_dicts: Географические словари
        
    Returns:
        Кортеж (скомпилированные паттерны, маппинг городов)
    """
    compiled_patterns = {}
    geo_city_map = {}
    
    # Russian cities - компилируем один большой паттерн
    russian_cities = geo_dicts.get('Russian', set())
    if russian_cities:
        sorted_cities = sorted(russian_cities, key=len, reverse=True)
        for city in sorted_cities:
            geo_city_map[city.lower()] = city
        escaped_cities = [re.escape(city.lower()) for city in sorted_cities]
        pattern = r'\b(' + '|'.join(escaped_cities) + r')\b'
        compiled_patterns['Russian'] = re.compile(pattern, re.IGNORECASE)
    
    # Moscow cities
    moscow_words = geo_dicts.get('Moscow', set())
    if moscow_words:
        sorted_moscow = sorted(moscow_words, key=len, reverse=True)
        escaped = [re.escape(w.lower()) for w in sorted_moscow]
        pattern = r'\b(' + '|'.join(escaped) + r')\b'
        compiled_patterns['Moscow'] = re.compile(pattern, re.IGNORECASE)
    
    # Other countries
    for geo_name in ['Kazakhstan', 'Belarus', 'Ukraine', 'Country']:
        geo_words = geo_dicts.get(geo_name, set())
        if geo_words:
            sorted_words = sorted(geo_words, key=len, reverse=True)
            escaped = [re.escape(w.lower()) for w in sorted_words]
            pattern = r'\b(' + '|'.join(escaped) + r')\b'
            compiled_patterns[geo_name] = re.compile(pattern, re.IGNORECASE)
    
    return compiled_patterns, geo_city_map


def extract_geo_info(
    query: str,
    compiled_geo_patterns: Dict[str, re.Pattern],
    geo_city_map: Dict[str, str]
) -> Dict[str, any]:
    """
    Извлекает географическую информацию с приоритетом для Russian.txt городов
    
    Args:
        query: Запрос
        compiled_geo_patterns: Скомпилированные паттерны
        geo_city_map: Маппинг городов
        
    Returns:
        Словарь с гео-информацией
    """
    query_lower = query.lower()
    
    geo_info = {
        'has_geo': False,
        'geo_type': None,
        'geo_country': None,
        'geo_city': None,
        'geo_region': None,
        'detected_location': None,
        'geo_street': None,
        'geo_house': None,
        'geo_full_address': None,
    }
    
    # Приоритет 1: Russian.txt - российские города
    if 'Russian' in compiled_geo_patterns:
        match = compiled_geo_patterns['Russian'].search(query_lower)
        if match:
            matched_city = match.group(1).lower()
            # Получаем оригинальное название города (с правильным регистром)
            # ВАЖНО: Город сохраняется как есть (lowercase), 
            # нормализация к красивому виду происходит на экспорте
            original_city = geo_city_map.get(matched_city, matched_city)
            geo_info.update({
                'has_geo': True,
                'geo_type': 'city',
                'geo_city': original_city,
                'geo_country': 'Russia',
                'detected_location': original_city
            })
            return geo_info
    
    # Приоритет 2: Moscow.txt - варианты Москвы
    if 'Moscow' in compiled_geo_patterns:
        match = compiled_geo_patterns['Moscow'].search(query_lower)
        if match:
            geo_info.update({
                'has_geo': True,
                'geo_type': 'city',
                'geo_city': 'Москва',
                'geo_country': 'Russia',
                'detected_location': match.group(1)
            })
            return geo_info
    
    # Приоритет 3: Другие страны
    for geo_name in ['Kazakhstan', 'Belarus', 'Ukraine']:
        if geo_name in compiled_geo_patterns:
            match = compiled_geo_patterns[geo_name].search(query_lower)
            if match:
                geo_info.update({
                    'has_geo': True,
                    'geo_type': 'country',
                    'geo_country': geo_name,
                    'detected_location': match.group(1)
                })
                return geo_info
    
    # Приоритет 4: Общие географические термины
    if 'Country' in compiled_geo_patterns:
        match = compiled_geo_patterns['Country'].search(query_lower)
        if match:
            geo_info.update({
                'has_geo': True,
                'geo_type': 'region',
                'detected_location': match.group(1)
            })
            return geo_info
    
    return geo_info


def has_geo(query_lower: str, geo_dicts: Dict[str, Set[str]]) -> bool:
    """Проверяет наличие географических маркеров"""
    for geo_dict in geo_dicts.values():
        if any(geo.lower() in query_lower for geo in geo_dict):
            return True
    return False
