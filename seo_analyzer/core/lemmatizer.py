"""
Lemmatizer Module
Оптимизированная лемматизация с кэшированием для всего проекта
"""

from functools import lru_cache
from typing import List


# Глобальный кэш для анализатора (singleton на уровне процесса)
_MORPH_ANALYZER = None


def get_morph_analyzer():
    """
    Получить singleton экземпляр MorphAnalyzer для процесса.
    
    Словари pymorphy3 загружаются один раз и остаются в RAM,
    что значительно ускоряет повторные обращения.
    
    Returns:
        pymorphy3.MorphAnalyzer: Экземпляр анализатора
    """
    global _MORPH_ANALYZER
    if _MORPH_ANALYZER is None:
        import pymorphy3
        _MORPH_ANALYZER = pymorphy3.MorphAnalyzer()
    return _MORPH_ANALYZER


@lru_cache(maxsize=10000)
def lemmatize_word(word: str) -> str:
    """
    Кэшированная лемматизация одного слова.
    
    Использует LRU кэш на 10000 слов для мгновенного получения
    результата для часто встречающихся слов.
    
    Args:
        word: Слово для лемматизации
        
    Returns:
        str: Нормальная форма слова
        
    Example:
        >>> lemmatize_word('купить')
        'купить'
        >>> lemmatize_word('купил')
        'купить'
        >>> lemmatize_word('покупает')
        'покупать'
    """
    if not word:
        return word
    
    morph = get_morph_analyzer()
    parsed = morph.parse(word)
    
    if parsed:
        return parsed[0].normal_form
    
    return word


@lru_cache(maxsize=5000)
def lemmatize_phrase(phrase: str, separator: str = ' ') -> str:
    """
    Кэшированная лемматизация фразы.
    
    Args:
        phrase: Фраза для лемматизации
        separator: Разделитель слов (по умолчанию пробел)
        
    Returns:
        str: Лемматизированная фраза
        
    Example:
        >>> lemmatize_phrase('купить систему контроля')
        'купить система контроль'
    """
    if not phrase:
        return phrase
    
    words = phrase.split(separator)
    lemmatized = [lemmatize_word(word) for word in words]
    
    return separator.join(lemmatized)


def lemmatize_words(words: List[str]) -> List[str]:
    """
    Лемматизация списка слов.
    
    Args:
        words: Список слов
        
    Returns:
        List[str]: Список лемматизированных слов
    """
    return [lemmatize_word(word) for word in words]


def clear_lemmatization_cache():
    """
    Очистить кэш лемматизации.
    
    Может быть полезно если нужно освободить память
    или сбросить кэш после обработки большого объема данных.
    """
    lemmatize_word.cache_clear()
    lemmatize_phrase.cache_clear()


def get_cache_info():
    """
    Получить информацию о состоянии кэша.
    
    Returns:
        dict: Статистика кэша (hits, misses, size, maxsize)
    """
    word_info = lemmatize_word.cache_info()
    phrase_info = lemmatize_phrase.cache_info()
    
    return {
        'word_cache': {
            'hits': word_info.hits,
            'misses': word_info.misses,
            'size': word_info.currsize,
            'maxsize': word_info.maxsize,
            'hit_rate': word_info.hits / (word_info.hits + word_info.misses) * 100 
                        if (word_info.hits + word_info.misses) > 0 else 0
        },
        'phrase_cache': {
            'hits': phrase_info.hits,
            'misses': phrase_info.misses,
            'size': phrase_info.currsize,
            'maxsize': phrase_info.maxsize,
            'hit_rate': phrase_info.hits / (phrase_info.hits + phrase_info.misses) * 100 
                        if (phrase_info.hits + phrase_info.misses) > 0 else 0
        }
    }



