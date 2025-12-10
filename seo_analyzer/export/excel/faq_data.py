"""Данные FAQ для Excel"""

from .faq_all_queries import get_all_queries_faq
from .faq_other_sheets import get_other_sheets_faq


def get_faq_data() -> list:
    """
    Получить данные для FAQ
    
    Returns:
        Список словарей с описанием столбцов
    """
    faq_data = []
    
    # Добавляем FAQ для листа 'Все запросы'
    faq_data.extend(get_all_queries_faq())
    
    # Добавляем FAQ для остальных листов
    faq_data.extend(get_other_sheets_faq())
    
    return faq_data
