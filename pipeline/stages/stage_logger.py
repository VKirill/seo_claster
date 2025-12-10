"""
Stage Logger
Логирование этапов с префиксом группы для параллельной обработки
"""


def get_group_prefix(analyzer) -> str:
    """
    Получить префикс группы для вывода
    
    Args:
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        Строка вида "[группа] " или пустая строка
    """
    if hasattr(analyzer, 'current_group') and analyzer.current_group:
        return f"[{analyzer.current_group.name}] "
    return ""


def print_stage(analyzer, message: str):
    """
    Вывод сообщения этапа с префиксом группы
    
    Args:
        analyzer: Экземпляр SEOAnalyzer
        message: Сообщение для вывода
    """
    prefix = get_group_prefix(analyzer)
    print(f"{prefix}{message}")


def print_stage_header(analyzer, title: str, width: int = 80):
    """
    Вывод заголовка этапа с префиксом группы
    
    Args:
        analyzer: Экземпляр SEOAnalyzer
        title: Заголовок этапа
        width: Ширина линии
    """
    prefix = get_group_prefix(analyzer)
    print(f"{prefix}{title}")
    print(f"{prefix}{'-' * width}")


def print_stage_separator(analyzer, width: int = 80):
    """
    Вывод разделителя с префиксом группы
    
    Args:
        analyzer: Экземпляр SEOAnalyzer
        width: Ширина линии
    """
    prefix = get_group_prefix(analyzer)
    print(f"{prefix}{'-' * width}")


