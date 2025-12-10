"""Модули для продвинутой дедупликации"""

from .deduplicator import AdvancedDeduplicator
from .text_processor import clean_text, extract_unique_words, get_signature, load_stopwords_from_file

__all__ = [
    'AdvancedDeduplicator',
    'clean_text',
    'extract_unique_words',
    'get_signature',
    'load_stopwords_from_file',
]

