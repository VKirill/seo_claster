"""
Обработка ошибок для SERP анализатора
"""

from typing import Dict, Any


def create_error_result(query: str, error: str, lr: int, empty_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Создать результат с ошибкой
    
    Args:
        query: Запрос
        error: Текст ошибки
        lr: Регион
        empty_metrics: Пустые метрики
        
    Returns:
        Словарь с результатом ошибки
    """
    return {
        'query': query,
        'lr': lr,
        'source': 'error',
        'error': error,
        'metrics': empty_metrics,
        'documents': [],
        'lsi_phrases': []
    }

