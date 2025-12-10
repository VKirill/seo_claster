"""Обработка текста для дедупликации"""

from typing import Set
import re
from pathlib import Path


def clean_text(text: str) -> str:
    """
    Очищает текст от спецсимволов
    Оставляет только буквы, цифры и пробелы
    
    Args:
        text: Исходный текст
        
    Returns:
        Очищенный текст
    """
    # Оставляем только буквы (кириллица, латиница), цифры и пробелы
    cleaned = re.sub(r'[^а-яёА-ЯЁa-zA-Z0-9\s]', ' ', text)
    # Убираем множественные пробелы
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip().lower()


def extract_unique_words(text: str, stopwords: Set[str] = None) -> Set[str]:
    """
    Извлекает уникальные слова из текста
    Исключает стоп-слова
    
    Args:
        text: Текст для обработки
        stopwords: Множество стоп-слов
        
    Returns:
        Множество уникальных значимых слов
    """
    if stopwords is None:
        stopwords = set()
    
    # Очищаем текст
    cleaned = clean_text(text)
    
    # Разбиваем на слова
    words = cleaned.split()
    
    # Фильтруем стоп-слова и короткие слова (< 2 символов)
    significant_words = {
        word for word in words
        if word not in stopwords and len(word) >= 2
    }
    
    return significant_words


def get_signature(text: str, stopwords: Set[str] = None) -> str:
    """
    Создает сигнатуру запроса для сравнения
    Сигнатура = отсортированный список уникальных слов
    
    Args:
        text: Исходный текст
        stopwords: Множество стоп-слов
        
    Returns:
        Строка-сигнатура
    """
    words = extract_unique_words(text, stopwords)
    # Сортируем для единообразия
    return ' '.join(sorted(words))


def load_stopwords_from_file(filepath: Path) -> Set[str]:
    """
    Загружает стоп-слова из файла
    
    Args:
        filepath: Путь к файлу со стоп-словами
        
    Returns:
        Множество стоп-слов в нижнем регистре
    """
    stopwords = set()
    
    if not filepath.exists():
        return stopwords
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            word = line.strip().lower()
            if word and len(word) >= 1:  # Включаем даже односимвольные предлоги
                stopwords.add(word)
    
    return stopwords

