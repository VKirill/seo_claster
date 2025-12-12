"""
Переводчик названий задач на русский язык
Используется для отображения технических названий задач в понятном виде
"""

from typing import Dict, Optional


# Маппинг технических названий задач на русские
TASK_NAME_TRANSLATIONS: Dict[str, str] = {
    # Генерация изображений
    'image_generation_photoshoot': 'Генерация изображений для фотосессии',
    'image_generation': 'Генерация изображений',
    'photoshoot_generation': 'Генерация фотосессии',
    
    # Анализ аватаров
    'avatar_analysis_result': 'Результат анализа аватара',
    'avatar_analysis': 'Анализ аватара',
    'avatar_result': 'Результат аватара',
    
    # Другие возможные задачи (можно расширить)
    'text_generation': 'Генерация текста',
    'image_processing': 'Обработка изображения',
    'content_analysis': 'Анализ контента',
    'data_processing': 'Обработка данных',
    'query_analysis': 'Анализ запросов',
    'seo_analysis': 'SEO анализ',
    'clustering': 'Кластеризация',
    'classification': 'Классификация',
    'intent_detection': 'Определение интента',
    'serp_analysis': 'SERP анализ',
    'direct_analysis': 'Yandex Direct анализ',
}


def translate_task_name(task_name: str) -> str:
    """
    Переводит техническое название задачи на русский язык
    
    Args:
        task_name: Техническое название задачи (например, 'image_generation_photoshoot')
        
    Returns:
        Русское название задачи или исходное название, если перевод не найден
    """
    if not task_name:
        return task_name
    
    # Прямой поиск в словаре
    if task_name in TASK_NAME_TRANSLATIONS:
        return TASK_NAME_TRANSLATIONS[task_name]
    
    # Попытка найти частичное совпадение (для составных названий)
    task_lower = task_name.lower()
    for key, translation in TASK_NAME_TRANSLATIONS.items():
        if key.lower() in task_lower or task_lower in key.lower():
            return translation
    
    # Если перевод не найден, возвращаем исходное название
    return task_name


def get_all_task_translations() -> Dict[str, str]:
    """
    Возвращает все доступные переводы названий задач
    
    Returns:
        Словарь с переводами
    """
    return TASK_NAME_TRANSLATIONS.copy()


def add_task_translation(task_name: str, translation: str) -> None:
    """
    Добавляет новый перевод названия задачи
    
    Args:
        task_name: Техническое название задачи
        translation: Русский перевод
    """
    TASK_NAME_TRANSLATIONS[task_name] = translation
