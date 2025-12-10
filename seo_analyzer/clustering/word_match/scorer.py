"""Подсчет скоров для кластеризации"""

from typing import Set


def count_matches(tokens1: Set[str], tokens2: Set[str]) -> int:
    """
    Подсчитывает количество совпадающих слов
    
    Args:
        tokens1: Множество токенов первой фразы
        tokens2: Множество токенов второй фразы
        
    Returns:
        Количество совпадений
    """
    return len(tokens1 & tokens2)

