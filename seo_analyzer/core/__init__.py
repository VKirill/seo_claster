"""Core модули для базовой обработки данных"""

from .xml_text_extractor import XMLTextExtractor
from .lemmatizer import (
    lemmatize_word,
    lemmatize_phrase,
    lemmatize_words,
    get_morph_analyzer,
    clear_lemmatization_cache,
    get_cache_info
)

__all__ = [
    'XMLTextExtractor',
    'lemmatize_word',
    'lemmatize_phrase',
    'lemmatize_words',
    'get_morph_analyzer',
    'clear_lemmatization_cache',
    'get_cache_info'
]
